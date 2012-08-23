/**
 * @file   std/src/path.c
 * @author vr <vr@bizanga.com>
 * @date   Mon May 31 11:17:13 2010
 * 
 * @brief  path mgmt routines
 * 
 * 
 */

#include <droplet.h>
#include "common.h"

dpl_status_t
dpltest_mkdir_check(dpl_ctx_t *ctx,
                    char *path)
{
  dpl_status_t ret_dpl;

  ret_dpl = dpl_getattr(ctx, path, NULL, NULL);
  if (DPL_SUCCESS != ret_dpl)
    {
      if (DPL_ENOENT != ret_dpl)
        {
          fprintf(stderr, "dpl_getattr %s failed: %s (%d)\n", path, dpl_status_str(ret_dpl), ret_dpl);
          return DPL_FAILURE;
        }
      else
        {
          ret_dpl = dpl_mkdir(ctx, path);
          if (DPL_SUCCESS != ret_dpl && DPL_EEXIST != ret_dpl)
            {
              fprintf(stderr, "dpl_mkdir %s failed: %s (%d)\n", path, dpl_status_str(ret_dpl), ret_dpl);
              return DPL_FAILURE;
            }
        }
    }

  return DPL_SUCCESS;
}

dpl_status_t
dpltest_path_make(dpl_ctx_t *ctx,
                  char *path,
                  int path_size,
                  char *base_path,
                  char *str,
                  char *ext,
                  int dir_hash_depth,
                  int do_mkdir)
{
  char *p;
  int str_len, len, base_path_len, ext_len, i;
  dpl_status_t ret_dpl;

  p = path;
  len = path_size;

  base_path_len = strlen(base_path);

  if (len < base_path_len)
    return DPL_FAILURE;
  memcpy(p, base_path, base_path_len); p += base_path_len; len -= base_path_len;

  if (len < 1)
    return DPL_FAILURE;
  *p = 0;

  if (do_mkdir)
    {
      ret_dpl = dpltest_mkdir_check(ctx, path);
      if (DPL_SUCCESS != ret_dpl)
        return DPL_FAILURE;
    }

  if (len < 1)
    return DPL_FAILURE;
  *p = '/'; p++; len--;

  for (i = 0;i < dir_hash_depth;i++)
    {
      if (len < 1)
        return DPL_FAILURE;
      *p = str[i]; p++; len--;

      if (len < 1)
        return DPL_FAILURE;
      *p = 0;

      if (do_mkdir)
        {
          ret_dpl = dpltest_mkdir_check(ctx, path);
          if (DPL_SUCCESS != ret_dpl)
            return DPL_FAILURE;
        }

      if (len < 1)
        return DPL_FAILURE;
      *p = '/'; p++; len--;
    }

  str_len = strlen(str);

  if (len < str_len)
    return DPL_FAILURE;
  memcpy(p, str, str_len); p += str_len; len -= str_len;

  if (NULL != ext)
    {
      if (len < 1)
        return DPL_FAILURE;
      *p = '.'; p++; len--;
      
      ext_len = strlen(ext);
      
      if (len < ext_len)
        return DPL_FAILURE;
      memcpy(p, ext, ext_len); p += ext_len; len -= ext_len;
    }

  if (len < 1)
    return DPL_FAILURE;
  *p = 0;
  
  return DPL_SUCCESS;
}
