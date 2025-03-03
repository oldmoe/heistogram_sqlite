#include <sqlite3ext.h>
#include "../heistogram/src/heistogram.h"

SQLITE_EXTENSION_INIT1

// a wrapper for heistogram_free
static inline void heistogram_free_wrapper(void *p) {
    heistogram_free((Heistogram *)p);
}

// Function prototypes
static void heist_create(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_group_create_step(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_aggregate_final(sqlite3_context *context);
static void heist_add(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_remove(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_merge(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_percentile(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_group_merge_step(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_group_add_step(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_group_remove_step(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_count(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_max(sqlite3_context *context, int argc, sqlite3_value **argv);
static void heist_min(sqlite3_context *context, int argc, sqlite3_value **argv);

/********************/
/* scalar functions */
/********************/

static void heist_create(sqlite3_context *context, int argc, sqlite3_value **argv) {
    Heistogram *h = heistogram_create();
    if (!h) {
        sqlite3_result_error(context, "Failed to create heistogram", -1);
        return;
    }
    size_t size;
    void *buffer = heistogram_serialize(h, &size);
    heistogram_free(h);
    if (!buffer) {
        sqlite3_result_error(context, "Failed to serialize heistogram", -1);
        return;
    }
    sqlite3_result_blob(context, buffer, size, free);
}

static void heist_add(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    Heistogram *h = heistogram_deserialize(buffer, size);
    if (!h) {
        sqlite3_result_error(context, "Failed to deserialize heistogram", -1);
        return;
    }
    uint64_t value = sqlite3_value_int64(argv[1]);
    heistogram_add(h, value);
    size_t new_size;
    void *new_buffer = heistogram_serialize(h, &new_size);
    heistogram_free(h);
    if (!new_buffer) {
        sqlite3_result_error(context, "Failed to serialize heistogram", -1);
        return;
    }
    sqlite3_result_blob(context, new_buffer, new_size, free);
}

static void heist_remove(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    Heistogram *h = heistogram_deserialize(buffer, size);
    if (!h) {
        sqlite3_result_error(context, "Failed to deserialize heistogram", -1);
        return;
    }
    uint64_t value = sqlite3_value_int64(argv[1]);
    heistogram_remove(h, value);
    size_t new_size;
    void *new_buffer = heistogram_serialize(h, &new_size);
    heistogram_free(h);
    if (!new_buffer) {
        sqlite3_result_error(context, "Failed to serialize heistogram", -1);
        return;
    }
    sqlite3_result_blob(context, new_buffer, new_size, free);
}

static void heist_merge(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer1 = (void *)sqlite3_value_blob(argv[0]);
    size_t size1 = sqlite3_value_bytes(argv[0]);
    void *buffer2 = (void *)sqlite3_value_blob(argv[1]);
    size_t size2 = sqlite3_value_bytes(argv[1]);
    
    Heistogram *h = heistogram_deserialize(buffer1, size1);
    if (!h) {
        sqlite3_result_error(context, "Failed to merge heistograms", -1);
        return;
    }
    heistogram_merge_inplace_serialized(h, buffer2, size2);
    size_t size;
    void *buffer = heistogram_serialize(h, &size);
    heistogram_free(h);
    if (!buffer) {
        sqlite3_result_error(context, "Failed to serialize heistogram", -1);
        return;
    }
    sqlite3_result_blob(context, buffer, size, free);
}

static void heist_percentile(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    double percentile = sqlite3_value_double(argv[1]);
    double result = heistogram_percentile_serialized(buffer, size, percentile);
    sqlite3_result_double(context, result);
}

static void heist_count(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, total_count);
}

static void heist_max(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, max);
}

static void heist_min(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, min);
}

static void heist_bucket_count(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, bucket_count);
}

static void heist_min_bucket(sqlite3_context *context, int argc, sqlite3_value **argv) {
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, min_bucket_id);
}

/***********************/
/* Aggregate functions */
/***********************/

// single finalizer for all aggregate functions that return a serialized heistogram
static void heist_aggregate_final(sqlite3_context *context) {
    Heistogram **h_ptr = (Heistogram **)sqlite3_aggregate_context(context, sizeof(Heistogram *));
    if (!*h_ptr) {
        sqlite3_result_error(context, "No heistogram created", -1);
        return;
    }
    size_t size;
    void *buffer = heistogram_serialize(*h_ptr, &size);
    heistogram_free(*h_ptr);
    if (!buffer) {
        sqlite3_result_error(context, "Failed to serialize heistogram", -1);
        return;
    }
    sqlite3_result_blob(context, buffer, size, free);
}    

static void heist_group_create_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    Heistogram **h_ptr = (Heistogram **)sqlite3_aggregate_context(context, sizeof(Heistogram *));
    if (!*h_ptr) {
        *h_ptr = heistogram_create();
        if (!*h_ptr) {
            sqlite3_result_error(context, "Failed to create heistogram", -1);
            return;
        }
    }
    uint64_t value = sqlite3_value_int64(argv[0]);
    heistogram_add(*h_ptr, value);
}

static void heist_group_add_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    Heistogram **h_ptr = (Heistogram **)sqlite3_aggregate_context(context, sizeof(Heistogram *));
    if (!*h_ptr) {
        *h_ptr = heistogram_create();
        if (!*h_ptr) {
            sqlite3_result_error(context, "Failed to create heistogram", -1);
            return;
        }
    }
    uint64_t value = sqlite3_value_int64(argv[0]);
    heistogram_add(*h_ptr, value);
}

static void heist_group_remove_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    Heistogram **h_ptr = (Heistogram **)sqlite3_aggregate_context(context, sizeof(Heistogram *));
    if (!*h_ptr) {
        *h_ptr = heistogram_create();
        if (!*h_ptr) {
            sqlite3_result_error(context, "Failed to create heistogram", -1);
            return;
        }
    }
    uint64_t value = sqlite3_value_int64(argv[0]);
    heistogram_remove(*h_ptr, value);
}

static void heist_group_merge_step(sqlite3_context *context, int argc, sqlite3_value **argv) {
    Heistogram **h_ptr = (Heistogram **)sqlite3_aggregate_context(context, sizeof(Heistogram *));
    if (!*h_ptr) {
        *h_ptr = heistogram_create();
        if (!*h_ptr) {
            sqlite3_result_error(context, "Failed to create heistogram", -1);
            return;
        }
    }
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);

    heistogram_merge_inplace_serialized(*h_ptr, buffer, size);
}


#ifdef _WIN32
__declspec(dllexport)
#endif

int sqlite3_heistogram_init(sqlite3 *db, char **pzErrMsg, const sqlite3_api_routines *pApi) {
    int rc = SQLITE_OK;
    SQLITE_EXTENSION_INIT2(pApi);

    rc = sqlite3_create_function(db, "heist_create", 0, SQLITE_UTF8, 0, heist_create, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_group_create", 1, SQLITE_UTF8, 0, 0, heist_group_create_step, heist_aggregate_final);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_add", 2, SQLITE_UTF8, 0, heist_add, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_remove", 2, SQLITE_UTF8, 0, heist_remove, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_merge", 2, SQLITE_UTF8, 0, heist_merge, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_percentile", 2, SQLITE_UTF8, 0, heist_percentile, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_group_merge", 1, SQLITE_UTF8, 0, 0, heist_group_merge_step, heist_aggregate_final);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_group_add", 1, SQLITE_UTF8, 0, 0, heist_group_add_step, heist_aggregate_final);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_group_add", 1, SQLITE_UTF8, 0, 0, heist_group_remove_step, heist_aggregate_final);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_count", 1, SQLITE_UTF8, 0, heist_count, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_max", 1, SQLITE_UTF8, 0, heist_max, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_min", 1, SQLITE_UTF8, 0, heist_min, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_bucket_count", 1, SQLITE_UTF8, 0, heist_bucket_count, 0, 0);
    if (rc != SQLITE_OK) return rc;

    rc = sqlite3_create_function(db, "heist_min_bucket", 1, SQLITE_UTF8, 0, heist_min_bucket, 0, 0);
    if (rc != SQLITE_OK) return rc;


    return rc;
}
