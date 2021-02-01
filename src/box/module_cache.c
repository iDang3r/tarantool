/*
 * SPDX-License-Identifier: BSD-2-Clause
 *
 * Copyright 2010-2021, Tarantool AUTHORS, please see AUTHORS file.
 */

#include <dlfcn.h>
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>

#include "assoc.h"
#include "diag.h"
#include "error.h"
#include "errinj.h"
#include "fiber.h"
#include "port.h"

#include "error.h"
#include "lua/utils.h"
#include "libeio/eio.h"
#include "trivia/util.h"

#include "module_cache.h"

/**
 * Modules names to descriptor hashes. The first one
 * for modules created with old `box.schema.func`
 * interface.
 *
 * Here is an important moment for backward compatibility.
 * The `box.schema.func` operations always use cache and
 * if a module is updated on a storage device or even
 * no longer present, then lazy symbol resolving is done
 * via previously loaded copy. To update modules one have
 * to reload them manually.
 *
 * In turn new API implies to use module_load/unload explicit
 * interface, and when module is re-loaded from cache then
 * we make a cache validation to be sure the copy on storage
 * is up to date.
 *
 * Due to all this we have to keep two hash tables. Probably
 * we should deprecate explicit reload at all and require
 * manual load/unload instead. But later.
 */
static struct mh_strnptr_t *box_schema_hash = NULL;
static struct mh_strnptr_t *mod_hash = NULL;

/**
 * Parsed symbol and package names.
 */
struct func_name {
	/**
	 * Null-terminated symbol name, e.g.
	 * "func" for "mod.submod.func".
	 */
	const char *sym;
	/**
	 * Package name, e.g. "mod.submod" for
	 * "mod.submod.func".
	 */
	const char *package;
	/**
	 * A pointer to the last character in ->package + 1.
	 */
	const char *package_end;
};

/**
 * Return module hash depending on where request comes
 * from: if it is legacy `box.schema.func` interface or not.
 */
static inline struct mh_strnptr_t *
hash_tbl(bool is_box_schema)
{
	return is_box_schema ? box_schema_hash : mod_hash;
}

/***
 * Split function name to symbol and package names.
 *
 * For example, str = foo.bar.baz => sym = baz, package = foo.bar
 *
 * @param str function name, e.g. "module.submodule.function".
 * @param[out] name parsed symbol and a package name.
 */
static void
func_split_name(const char *str, struct func_name *name)
{
	name->package = str;
	name->package_end = strrchr(str, '.');
	if (name->package_end != NULL) {
		/* module.submodule.function => module.submodule, function */
		name->sym = name->package_end + 1; /* skip '.' */
	} else {
		/* package == function => function, function */
		name->sym = name->package;
		name->package_end = str + strlen(str);
	}
}

/**
 * Look up a module in the modules cache.
 */
static struct module *
module_cache_find(struct mh_strnptr_t *h, const char *name,
		  const char *name_end)
{
	mh_int_t e = mh_strnptr_find_inp(h, name, name_end - name);
	if (e == mh_end(h))
		return NULL;
	return mh_strnptr_node(h, e)->val;
}

/**
 * Save a module to the modules cache.
 */
static int
module_cache_add(struct module *module)
{
	struct mh_strnptr_t *h = module->hash;
	size_t package_len = strlen(module->package);
	const struct mh_strnptr_node_t nd = {
		.str	= module->package,
		.len	= package_len,
		.hash	= mh_strn_hash(module->package, package_len),
		.val	= module,
	};

	if (mh_strnptr_put(h, &nd, NULL, NULL) == mh_end(h)) {
		diag_set(OutOfMemory, sizeof(nd), "malloc",
			 "module cache node");
		return -1;
	}
	return 0;
}

/**
 * Update the module cache. Since the lookup is string
 * key based it is safe to just update the value.
 */
static int
module_cache_update(struct module *module)
{
	struct mh_strnptr_t *h = module->hash;
	const char *name = module->package;
	size_t len = strlen(module->package);

	mh_int_t e = mh_strnptr_find_inp(h, name, len);
	if (e == mh_end(h))
		return -1;
	mh_strnptr_node(h, e)->str = module->package;
	mh_strnptr_node(h, e)->val = module;
	return 0;
}

/**
 * Delete a module from the modules cache.
 */
static void
module_cache_del(struct module *module)
{
	struct mh_strnptr_t *h = module->hash;
	const char *name = module->package;
	size_t len = strlen(module->package);

	mh_int_t e = mh_strnptr_find_inp(h, name, len);
	if (e != mh_end(h))
		mh_strnptr_del(h, e, NULL);
}

/**
 * Mark module as out of cache.
 */
static void
module_set_orphan(struct module *module)
{
	module->hash = NULL;
}

/**
 * Test if module is out of cache.
 */
bool
module_is_orphan(struct module *module)
{
	return module->hash == NULL;
}

/**
 * Arguments for luaT_module_find used by lua_cpcall().
 */
struct module_find_ctx {
	const char *package;
	const char *package_end;
	char *path;
	size_t path_len;
};

/**
 * A cpcall() helper for module_find().
 */
static int
luaT_module_find(lua_State *L)
{
	struct module_find_ctx *ctx = (void *)lua_topointer(L, 1);

	/*
	 * Call package.searchpath(name, package.cpath) and use
	 * the path to the function in dlopen().
	 */
	lua_getglobal(L, "package");
	lua_getfield(L, -1, "search");

	/* Argument of search: name */
	lua_pushlstring(L, ctx->package, ctx->package_end - ctx->package);

	lua_call(L, 1, 1);
	if (lua_isnil(L, -1))
		return luaL_error(L, "module not found");

	/* Convert path to absolute */
	char resolved[PATH_MAX];
	if (realpath(lua_tostring(L, -1), resolved) == NULL) {
		diag_set(SystemError, "realpath");
		return luaT_error(L);
	}

	snprintf(ctx->path, ctx->path_len, "%s", resolved);
	return 0;
}

/**
 * Find a path to a module using Lua's package.cpath.
 *
 * @param package package name
 * @param package_end a pointer to the last byte in @a package + 1
 * @param[out] path path to shared library
 * @param path_len size of @a path buffer
 *
 * @retval 0 on success
 * @retval -1 on error, diag is set
 */
static int
module_find(const char *package, const char *package_end,
	    char *path, size_t path_len)
{
	struct module_find_ctx ctx = {
		.package	= package,
		.package_end	= package_end,
		.path		= path,
		.path_len	= path_len,
	};
	lua_State *L = tarantool_L;
	int top = lua_gettop(L);
	if (luaT_cpcall(L, luaT_module_find, &ctx) != 0) {
		diag_set(ClientError, ER_LOAD_MODULE,
			 (int)(package_end - package),
			 package, lua_tostring(L, -1));
		lua_settop(L, top);
		return -1;
	}
	assert(top == lua_gettop(L)); /* cpcall discard results */
	return 0;
}

/**
 * Delete a module.
 */
static void
module_delete(struct module *module)
{
	struct errinj *e = errinj(ERRINJ_DYN_MODULE_COUNT, ERRINJ_INT);
	if (e != NULL)
		--e->iparam;
	dlclose(module->handle);
	TRASH(module);
	free(module);
}

/**
 * Increase reference to a module.
 */
static void
module_ref(struct module *module)
{
	assert(module->refs >= 0);
	module->refs++;
}

/**
 * Decrease module reference and delete it if last one.
 */
static void
module_unref(struct module *module)
{
	assert(module->refs > 0);
	if (module->refs-- == 1) {
		if (!module_is_orphan(module))
			module_cache_del(module);
		module_delete(module);
	}
}

/**
 * Load dynamic shared object, ie module library.
 *
 * Create a new symlink based on temporary directory
 * and try to load via this symink to load a dso twice
 * for cases of a function reload.
 */
static struct module *
module_new(const char *path, struct mh_strnptr_t *h,
	   const char *package, const char *package_end)
{
	int package_len = package_end - package;
	struct module *module = malloc(sizeof(*module) + package_len + 1);
	if (module == NULL) {
		diag_set(OutOfMemory, sizeof(*module) + package_len + 1,
			 "malloc", "struct module");
		return NULL;
	}
	memcpy(module->package, package, package_len);
	module->package[package_len] = 0;
	rlist_create(&module->funcs_list);
	module->refs = 0;
	module->hash = h;

	const char *tmpdir = getenv("TMPDIR");
	if (tmpdir == NULL)
		tmpdir = "/tmp";

	char dir_name[PATH_MAX];
	int rc = snprintf(dir_name, sizeof(dir_name), "%s/tntXXXXXX", tmpdir);
	if (rc < 0 || (size_t)rc >= sizeof(dir_name)) {
		diag_set(SystemError, "failed to generate path to tmp dir");
		goto error;
	}

	if (mkdtemp(dir_name) == NULL) {
		diag_set(SystemError, "failed to create unique dir name: %s",
			 dir_name);
		goto error;
	}

	char load_name[PATH_MAX];
	rc = snprintf(load_name, sizeof(load_name), "%s/%.*s." TARANTOOL_LIBEXT,
		      dir_name, package_len, package);
	if (rc < 0 || (size_t)rc >= sizeof(dir_name)) {
		diag_set(SystemError, "failed to generate path to DSO");
		goto error;
	}

	struct stat *st = &module->st;
	if (stat(path, st) < 0) {
		diag_set(SystemError, "failed to stat() module %s", path);
		goto error;
	}

	int source_fd = open(path, O_RDONLY);
	if (source_fd < 0) {
		diag_set(SystemError, "failed to open module %s", path);
		goto error;
	}

	int dest_fd = open(load_name, O_WRONLY | O_CREAT | O_TRUNC,
			   st->st_mode & (S_IRWXU | S_IRWXG | S_IRWXO));
	if (dest_fd < 0) {
		diag_set(SystemError, "failed to open file %s for writing ",
			 load_name);
		close(source_fd);
		goto error;
	}

	off_t ret = eio_sendfile_sync(dest_fd, source_fd, 0, st->st_size);
	close(source_fd);
	close(dest_fd);
	if (ret != st->st_size) {
		diag_set(SystemError, "failed to copy DSO %s to %s",
			 path, load_name);
		goto error;
	}

	module->handle = dlopen(load_name, RTLD_NOW | RTLD_LOCAL);
	if (unlink(load_name) != 0)
		say_warn("failed to unlink dso link %s", load_name);
	if (rmdir(dir_name) != 0)
		say_warn("failed to delete temporary dir %s", dir_name);
	if (module->handle == NULL) {
		diag_set(ClientError, ER_LOAD_MODULE, package_len,
			  package, dlerror());
		goto error;
	}

	struct errinj *e = errinj(ERRINJ_DYN_MODULE_COUNT, ERRINJ_INT);
	if (e != NULL)
		++e->iparam;
	module_ref(module);
	return module;

error:
	free(module);
	return NULL;
}

/**
 * Import a function from a module.
 */
static box_function_f
module_sym(struct module *module, const char *name)
{
	box_function_f f = dlsym(module->handle, name);
	if (f == NULL) {
		diag_set(ClientError, ER_LOAD_FUNCTION, name, dlerror());
		return NULL;
	}
	return f;
}

int
module_sym_load(struct module_sym *mod_sym, bool is_box_schema)
{
	struct module *cached, *module;
	assert(mod_sym->addr == NULL);

	struct func_name name;
	func_split_name(mod_sym->name, &name);

	if (is_box_schema) {
		/*
		 * Deprecated interface -- request comes
		 * from box.schema.func.
		 *
		 * In case if module has been loaded already by
		 * some previous call we can eliminate redundant
		 * loading and take it from the cache.
		 */
		struct mh_strnptr_t *h = hash_tbl(is_box_schema);
		cached = module_cache_find(h, name.package, name.package_end);
		if (cached == NULL) {
			char path[PATH_MAX];
			if (module_find(name.package, name.package_end,
					path, sizeof(path)) != 0) {
				return -1;
			}
			module = module_new(path, h, name.package,
					    name.package_end);
			if (module == NULL)
				return -1;
			if (module_cache_add(module) != 0) {
				module_unref(module);
				return -1;
			}
		} else {
			module_ref(cached);
			module = cached;
		}
		mod_sym->module = module;
	} else {
		/*
		 * New approach is always load module
		 * explicitly and pass it inside symbol,
		 * the refernce to the module already has
		 * to be incremented.
		 */
		assert(mod_sym->module->refs > 0);
		module_ref(mod_sym->module);
		module = mod_sym->module;
	}

	mod_sym->addr = module_sym(module, name.sym);
	if (mod_sym->addr == NULL) {
		module_unref(module);
		return -1;
	}

	rlist_add(&module->funcs_list, &mod_sym->item);
	return 0;
}

void
module_sym_unload(struct module_sym *mod_sym)
{
	if (mod_sym->addr == NULL)
		return;

	rlist_del(&mod_sym->item);
	/*
	 * Unref action might delete module
	 * so call it after rlist_del.
	 */
	module_unref(mod_sym->module);

	mod_sym->module = NULL;
	mod_sym->addr = NULL;
}

int
module_sym_call(struct module_sym *mod_sym, struct port *args,
		struct port *ret)
{
	/*
	 * The functions created with `box.schema.func`
	 * help are not resolved immediately. Instead
	 * they are deferred until first call. And when
	 * call happens the we try to load a module and
	 * resolve a symbol (which of course can fail if
	 * there is no such module at all).
	 *
	 * While this is very weird (and frankly speaking
	 * very bad design) we can't change it for backward
	 * compatibility sake!
	 */
	if (mod_sym->addr == NULL) {
		if (module_sym_load(mod_sym, true) != 0)
			return -1;
	}

	struct region *region = &fiber()->gc;
	size_t region_svp = region_used(region);

	uint32_t data_sz;
	const char *data = port_get_msgpack(args, &data_sz);
	if (data == NULL)
		return -1;

	port_c_create(ret);
	box_function_ctx_t ctx = {
		.port = ret,
	};

	/*
	 * Module can be changed after function reload. Also
	 * keep in mind that stored C procedure may yield inside.
	 */
	struct module *module = mod_sym->module;
	assert(module != NULL);
	module_ref(module);
	int rc = mod_sym->addr(&ctx, data, data + data_sz);
	module_unref(module);
	region_truncate(region, region_svp);

	if (rc != 0) {
		if (diag_last_error(&fiber()->diag) == NULL) {
			/* Stored procedure forget to set diag  */
			diag_set(ClientError, ER_PROC_C, "unknown error");
		}
		port_destroy(ret);
		return -1;
	}

	return rc;
}

struct module *
module_load(const char *package, const char *package_end)
{
	char path[PATH_MAX];
	if (module_find(package, package_end, path, sizeof(path)) != 0)
		return NULL;

	struct module *cached, *module;
	struct mh_strnptr_t *h = hash_tbl(false);
	cached = module_cache_find(h, package, package_end);
	if (cached == NULL) {
		module = module_new(path, h, package, package_end);
		if (module == NULL)
			return NULL;
		if (module_cache_add(module) != 0) {
			module_unref(module);
			return NULL;
		}
		return module;
	}

	struct stat st;
	if (stat(path, &st) != 0) {
		diag_set(SystemError, "module: stat() module %s", path);
		return NULL;
	}

	/*
	 * When module comes from cache make sure that
	 * it is not changed on the storage device. The
	 * test below still can miss update if cpu data
	 * been manually moved backward and device/inode
	 * persisted but this is a really rare situation.
	 *
	 * If update is needed one can simply "touch file.so"
	 * to invalidate the cache entry.
	 */
	if (cached->st.st_dev == st.st_dev &&
	    cached->st.st_ino == st.st_ino &&
	    cached->st.st_size == st.st_size &&
	    memcmp(&cached->st.st_mtim, &st.st_mtim,
		   sizeof(st.st_mtim)) == 0) {
		module_ref(cached);
		return cached;
	}

	/*
	 * Load a new module, update the cache
	 * and orphan an old module instance.
	 */
	module = module_new(path, h, package, package_end);
	if (module == NULL)
		return NULL;
	if (module_cache_update(module) != 0) {
		module_unref(module);
		return NULL;
	}

	module_set_orphan(cached);
	return module;
}

void
module_unload(struct module *module)
{
	module_unref(module);
}

int
module_reload(const char *package, const char *package_end)
{
	struct module *old, *new;

	/*
	 * Explicit module reloading is deprecated interface,
	 * so always use box_schema_hash.
	 */
	old = module_cache_find(box_schema_hash, package, package_end);
	if (old == NULL) {
		diag_set(ClientError, ER_NO_SUCH_MODULE, package);
		return -1;
	}

	char path[PATH_MAX];
	if (module_find(package, package_end, path, sizeof(path)) != 0)
		return -1;

	new = module_new(path, box_schema_hash, package, package_end);
	if (new == NULL)
		return -1;

	/* Extra ref until cache get updated */
	module_ref(old);

	struct module_sym *mod_sym, *tmp;
	rlist_foreach_entry_safe(mod_sym, &old->funcs_list, item, tmp) {
		struct func_name name;
		func_split_name(mod_sym->name, &name);

		mod_sym->addr = module_sym(new, name.sym);
		if (mod_sym->addr == NULL) {
			say_error("module: reload %s, symbol %s not found",
				  package, name.sym);
			goto restore;
		}

		mod_sym->module = new;
		rlist_move(&new->funcs_list, &mod_sym->item);

		/* Move reference to a new place */
		module_ref(new);
		module_unref(old);
	}

	if (module_cache_update(new) != 0) {
		/*
		 * Module cache must be consistent at this moment,
		 * we've looked up for the package recently. If
		 * someone has updated the cache in unexpected way
		 * the consistency is lost and we must not continue.
		 */
		panic("module: can't update module cache (%s)", package);
	}

	module_set_orphan(old);
	module_unref(old);

	/* From explicit load above */
	module_unref(new);
	return 0;

restore:
	module_set_orphan(new);
	/*
	 * Some old-dso func can't be load from new module,
	 * restore old functions.
	 */
	do {
		struct func_name name;
		func_split_name(mod_sym->name, &name);
		mod_sym->addr = module_sym(old, name.sym);
		if (mod_sym->addr == NULL) {
			/*
			 * Something strange was happen, an early loaden
			 * function was not found in an old dso.
			 */
			panic("Can't restore module function, "
			      "server state is inconsistent");
		}
		mod_sym->module = old;
		rlist_move(&old->funcs_list, &mod_sym->item);
		module_ref(old);
		module_unref(new);
	} while (mod_sym != rlist_first_entry(&old->funcs_list,
					      struct module_sym,
					      item));
	assert(rlist_empty(&new->funcs_list));
	module_unref(new);
	return -1;
}

int
module_init(void)
{
	struct mh_strnptr_t **ht[] = {
		&box_schema_hash,
		&mod_hash,
	};
	for (size_t i = 0; i < lengthof(ht); i++) {
		*ht[i] = mh_strnptr_new();
		if (*ht[i] == NULL) {
			diag_set(OutOfMemory, sizeof(*ht[i]),
				 "malloc", "modules hash");
			for (ssize_t j = i - 1; j >= 0; j--) {
				mh_strnptr_delete(*ht[j]);
				*ht[j] = NULL;
			}
			return -1;
		}
	}
	return 0;
}

void
module_free(void)
{
	struct mh_strnptr_t **ht[] = {
		&box_schema_hash,
		&mod_hash,
	};
	for (size_t i = 0; i < lengthof(ht); i++) {
		struct mh_strnptr_t *h = *ht[i];

		mh_int_t i = mh_first(h);
		struct module *m = mh_strnptr_node(h, i)->val;
		module_unref(m);

		mh_strnptr_delete(h);
		*ht[i] = NULL;
	}
}