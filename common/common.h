
typedef signed char s8;
typedef unsigned char u8;
typedef signed short s16;
typedef unsigned short u16;
typedef signed int s32;
typedef unsigned int u32;
typedef signed long long s64;
typedef unsigned long long u64;

void gen_data(char *id, char *buf, int len);
int check_data(char *id, char *buf, int len);

u64 get_oid(int oflag, struct drand48_data *pdrbuffer);

void rand_str(char *str, int len);
