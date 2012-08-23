
typedef signed char s8;
typedef unsigned char u8;
typedef signed short s16;
typedef unsigned short u16;
typedef signed int s32;
typedef unsigned int u32;
typedef signed long long s64;
typedef unsigned long long u64;

pid_t gettid();

dpl_status_t
dpltest_gen_file_name_from_oid(u64 oid, char **file_namep);

void dpltest_gen_data(char *id, char *buf, int len);
int dpltest_check_data(char *id, char *buf, int len);

u64 dpltest_get_oid(int oflag, struct drand48_data *pdrbuffer);

void dpltest_rand_str(char *str, int len);

dpl_status_t
dpltest_path_make(dpl_ctx_t *ctx,
                  char *path,
                  int path_size,
                  char *base_path,
                  char *str,
                  char *ext,
                  int dir_hash_depth,
                  int do_mkdir);

dpl_status_t
dpltest_upload_file(dpl_ctx_t *ctx,
                    char *path,
                    char *blob,
                    int blob_size,
                    int buffered,
                    int block_size);
