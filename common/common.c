
#include <droplet.h>
#include <pthread.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>
#include <syscall.h>
#include <sys/param.h>
#include "common.h"

pid_t 
gettid()
{
  return syscall(SYS_gettid);
}

#define DPLTEST_CLASS_NBITS            4
#define DPLTEST_REPLICA_NBITS          4
#define DPLTEST_EXTRA_NBITS            (DPLTEST_CLASS_NBITS+DPLTEST_REPLICA_NBITS)

#define DPLTEST_SPECIFIC_NBITS       24
#define DPLTEST_SERVICEID_NBITS      8
#define DPLTEST_VOLID_NBITS          32
#define DPLTEST_OID_NBITS            64
#define DPLTEST_HASH_NBITS           24 /*!< dispersion */

#define DPLTEST_PAYLOAD_NBITS        (DPLTEST_SPECIFIC_NBITS+DPLTEST_SERVICEID_NBITS+DPLTEST_VOLID_NBITS+DPLTEST_OID_NBITS)

#define DPLTEST_SERVICE_ID_TEST     0x00

#define BIT_SET(bit)   (entropy[(bit)/NBBY] |= (1<<((bit)%NBBY)))
#define BIT_CLEAR(bit) (entropy[(bit)/NBBY] &= ~(1<<((bit)%NBBY)))

static dpl_status_t
dpltest_gen_key(BIGNUM *id,
                uint64_t oid,
                uint32_t volid,
                uint8_t serviceid,
                uint32_t specific)
{
  int off, i;
  MD5_CTX ctx;
  char entropy[DPLTEST_PAYLOAD_NBITS/NBBY];
  u_char hash[MD5_DIGEST_LENGTH];

  BN_zero(id);
  
  off = DPLTEST_REPLICA_NBITS +
    DPLTEST_CLASS_NBITS;

  for (i = 0;i < DPLTEST_SPECIFIC_NBITS;i++)
    {
      if (specific & 1<<i)
        {
          BN_set_bit(id, off + i);
          BIT_SET(off - DPLTEST_EXTRA_NBITS + i);
        }
      else
        {
          BN_clear_bit(id, off + i);
          BIT_CLEAR(off - DPLTEST_EXTRA_NBITS + i);
        }
    }

  off += DPLTEST_SPECIFIC_NBITS;

  for (i = 0;i < DPLTEST_SERVICEID_NBITS;i++)
    {
      if (serviceid & 1<<i)
        {
          BN_set_bit(id, off + i);
          BIT_SET(off - DPLTEST_EXTRA_NBITS + i);
        }
      else
        {
          BN_clear_bit(id, off + i);
          BIT_CLEAR(off - DPLTEST_EXTRA_NBITS + i);
        }
    }

  off += DPLTEST_SERVICEID_NBITS;

  for (i = 0;i < DPLTEST_VOLID_NBITS;i++)
    {
      if (volid & 1<<i)
        {
          BN_set_bit(id, off + i);
          BIT_SET(off - DPLTEST_EXTRA_NBITS + i);
        }
      else
        {
          BN_clear_bit(id, off + i);
          BIT_CLEAR(off - DPLTEST_EXTRA_NBITS + i);
        }
    }

  off += DPLTEST_VOLID_NBITS;

  for (i = 0;i < DPLTEST_OID_NBITS;i++)
    {
      if (oid & (1ULL<<i))
        {
          BN_set_bit(id, off + i);
          BIT_SET(off - DPLTEST_EXTRA_NBITS + i);
        }
      else
        {
          BN_clear_bit(id, off + i);
          BIT_CLEAR(off  - DPLTEST_EXTRA_NBITS + i);
        }
    }

  off += DPLTEST_OID_NBITS;

  MD5_Init(&ctx);
  MD5_Update(&ctx, entropy, sizeof (entropy));
  MD5_Final(hash, &ctx);

  for (i = 0;i < DPLTEST_HASH_NBITS;i++)
    {
      if (hash[i/8] & 1<<(i%8))
        BN_set_bit(id, off + i);
      else
        BN_clear_bit(id, off + i);
    }

  return DPL_SUCCESS;
}

dpl_status_t
dpltest_gen_file_name_from_oid(u64 oid, 
                               char **file_namep)
{
  BIGNUM *bn = NULL;
  int ret, ret2;
  char *file_name = NULL;

  bn = BN_new();
  if (NULL == bn)
    {
      ret = DPL_ENOMEM;
      goto end;
    }

  ret2 = dpltest_gen_key(bn, oid, 0, DPLTEST_SERVICE_ID_TEST, 0);
  if (DPL_SUCCESS != ret2)
    {
      ret = ret2;
      goto end;
    }

  file_name = BN_bn2hex(bn);
  if (NULL == file_name)
    {
      ret = DPL_ENOMEM;
      goto end;
    }

  if (NULL != file_namep)
    {
      *file_namep = file_name;
      file_name = NULL;
    }

  ret = DPL_SUCCESS;

 end:

  if (NULL != file_name)
    free(file_name);

  if (NULL != bn)
    BN_free(bn);
  
  return ret;
}

void 
dpltest_gen_data(char *id,
                 char *buf,
                 int len)
{
  char *p;
  char *e;
  int i, id_len, buffered_len, remain_len;

  id_len = strlen(id);

  p = id;
  buffered_len = (len/id_len)*id_len;
  remain_len = len - buffered_len;
  e = buf + buffered_len;
  for (;buf < e; buf += id_len) 
    {
      memcpy(buf, p, id_len);
    }
  for (i=0;i < remain_len;i++) 
    {
      buf[i] = p[i%id_len];
    }
}

int
dpltest_check_data(char *id, 
                   char *buf,
                   int len)
{
  char *p;
  int i, id_len;

  id_len = strlen(id);

  p = id;
  for (i = 0;i < len;i++)
    {
      if (buf[i] != p[i%id_len])
        {
          fprintf(stderr,
                  "byte %d differ: %d != %d\n", i, buf[i], p[i%id_len]);
          return -1;
        }
    }

  return 0;
}

u64
dpltest_get_oid(int oflag,
                struct drand48_data *pdrbuffer)
{
  static u64 static_oid = 0;
  static pthread_mutex_t oid_lock = PTHREAD_MUTEX_INITIALIZER;
  u64 oid;

  if (1 == oflag)
    {
      long int tmp;
      pthread_mutex_lock(&oid_lock);
      lrand48_r(pdrbuffer, &tmp);
      oid = tmp;
      pthread_mutex_unlock(&oid_lock);
      return oid;
    }
  else
    {
      pthread_mutex_lock(&oid_lock);
      oid = static_oid++;
      pthread_mutex_unlock(&oid_lock);

      return oid;
    }
}

void 
dpltest_rand_str(char *str, int len) 
{
  int i;

  for (i = 0;i < len;i++) 
    str[i] = 'a' + rand()%26; 

  str[i] = 0;
}
