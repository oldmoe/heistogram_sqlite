#include <sqlite3ext.h>
#include "../heistogram/src/heistogram.h"

SQLITE_EXTENSION_INIT1

enum header_value_name {
    HEIST_COUNT,            
    HEIST_MIN,                  
    HEIST_MAX,     
    HEIST_BUCKET_COUNT,
    HEIST_MIN_BUCKET   
};

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
    
    for (int i = 0; i < argc; i++) {
        if (sqlite3_value_type(argv[i]) != SQLITE_NULL) {
            uint64_t value = sqlite3_value_int64(argv[i]);
            heistogram_add(h, value);
        }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL){
        sqlite3_result_blob(context, buffer, size, SQLITE_TRANSIENT);
        return;
    }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    if (sqlite3_value_type(argv[1]) == SQLITE_NULL){
        sqlite3_result_blob(context, buffer, size, SQLITE_TRANSIENT);
        return;
    }
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
    if (!buffer1 && buffer2) {
        sqlite3_result_blob(context, buffer2, size2, SQLITE_TRANSIENT);
        return;
    }else if(!buffer2 && buffer1){
        sqlite3_result_blob(context, buffer1, size1, SQLITE_TRANSIENT);
        return;
    }else if(!buffer1 && !buffer2){
        sqlite3_result_null(context);
        return;
    }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL || sqlite3_value_type(argv[1]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    size_t size = sqlite3_value_bytes(argv[0]);
    double percentile = sqlite3_value_double(argv[1]);
    double result = heistogram_percentile_serialized(buffer, size, percentile);
    sqlite3_result_double(context, result);
}

static void heist_get_header_value(sqlite3_context *context, sqlite3_value *argument, enum header_value_name value_name){
    if (sqlite3_value_type(argument) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    void *buffer = (void *)sqlite3_value_blob(argument);
    if(! buffer){
        sqlite3_result_null(context);
        return;
    }
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    int64_t result = -1;
    switch(value_name){
        case HEIST_COUNT:
            result = total_count;
            break;
        case HEIST_MAX:
            result = max;
            break;
        case HEIST_MIN:
            result = min;
            break;
        case HEIST_BUCKET_COUNT:
            result =  (int64_t)bucket_count;
            break;
        case HEIST_MIN_BUCKET:
            result = (int64_t)min_bucket_id;        
    }
    if(result == -1) {
        sqlite3_result_null(context);
    } else {
        sqlite3_result_int64(context, result);
    }
}

static void heist_count(sqlite3_context *context, int argc, sqlite3_value **argv) {
    //heist_get_header_value(context, argv[0], HEIST_COUNT);
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        sqlite3_result_null(context);
        return;
    }
    void *buffer = (void *)sqlite3_value_blob(argv[0]);
    if(! buffer){
        sqlite3_result_null(context);
        return;
    }
    uint16_t bucket_count;
    uint64_t total_count;
    uint64_t min;
    uint64_t max;
    uint16_t min_bucket_id;
    decode_header(buffer, &bucket_count, &total_count, &min, &max, &min_bucket_id);
    sqlite3_result_int64(context, total_count);
}

static void heist_max(sqlite3_context *context, int argc, sqlite3_value **argv) {
    heist_get_header_value(context, argv[0], HEIST_MAX);
}

static void heist_min(sqlite3_context *context, int argc, sqlite3_value **argv) {
    heist_get_header_value(context, argv[0], HEIST_MIN);
}

static void heist_bucket_count(sqlite3_context *context, int argc, sqlite3_value **argv) {
    heist_get_header_value(context, argv[0], HEIST_BUCKET_COUNT);
}

static void heist_min_bucket(sqlite3_context *context, int argc, sqlite3_value **argv) {
    heist_get_header_value(context, argv[0], HEIST_MIN_BUCKET);
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return; // Skip NULL values
    }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return; // Skip NULL values
    }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return; // Skip NULL values
    }
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
    if (sqlite3_value_type(argv[0]) == SQLITE_NULL) {
        return; // Skip NULL values
    }
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

    rc = sqlite3_create_function(db, "heist_create", -1, SQLITE_UTF8, 0, heist_create, 0, 0);
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
