/** @file

  A brief file description

  @section license License

  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */

/* throttle.c:  a program that does a throttle transform
 */

#include <stdio.h>
#include <unistd.h>
#include <ts/ts.h>
//#include <ts/ink_bool.h>  todo : why make can't locate ink_bool.h ?

// This gets the PRI*64 types
# define __STDC_FORMAT_MACROS 1
# include <inttypes.h>
#include <string.h>
#include <sys/time.h>
#include <stdlib.h>
#include <time.h>

#define THROTTLE_DIR       "/usr/local/etc/trafficserver/cloudplugin/throttle"
#define	BPDCOUNT	1024
#define MAX_DOMAIN_LEN	1024
typedef struct
{
	char	domain[MAX_DOMAIN_LEN];
	int		domain_length;
	float	f_rate;
} bitrate_per_domain;

static bitrate_per_domain bpd[BPDCOUNT];
static int	bpdcount = 0;

typedef struct
{
	// these are for sending response to browser.
	TSVIO				output_vio;
	TSIOBuffer			output_buffer;
	TSIOBufferReader	output_reader;

	float	f_rate;
#ifdef DEBUG
	int64_t	i64_header_amount;
	int64_t	i64_total_sent;
	long	l_starttime, l_endtime;
#endif
} MyData;

static MyData *
my_data_alloc()
{
  MyData *data;

  data = (MyData *) TSmalloc(sizeof(MyData));
  TSReleaseAssert(data);
  
  data->output_vio = NULL;
  data->output_buffer = NULL;
  data->output_reader = NULL;
  data->f_rate = 0;
#ifdef DEBUG
  data->i64_header_amount = 0;
  data->i64_total_sent = 0;
  data->l_starttime = 0;
  data->l_endtime = 0;
#endif

  return data;
}

static void
my_data_destroy(MyData * data)
{
  if (data) {
    if (data->output_buffer) TSIOBufferDestroy(data->output_buffer);
	if (data->output_reader) TSIOBufferReaderFree(data->output_reader);
    TSfree(data);
  }
}

#ifdef DEBUG
static inline long myclock()
{
	struct timeval tv;
	gettimeofday (&tv, NULL);
	return (tv.tv_sec);
}
#endif

static void
handle_transform(TSCont contp)
{
  TSVConn output_conn;
  TSIOBuffer buf_test;
  TSVIO input_vio;
  MyData *data;
  int64_t towrite;
  int64_t avail;

  /* Get the output (downstream) vconnection where we'll write data to. */

  output_conn = TSTransformOutputVConnGet(contp);

  /* Get the write VIO for the write operation that was performed on
   * ourself. This VIO contains the buffer that we are to read from
   * as well as the continuation we are to call when the buffer is
   * empty. This is the input VIO (the write VIO for the upstream
   * vconnection).
   */
  input_vio = TSVConnWriteVIOGet(contp);

  /* Get our data structure for this operation. The private data
   * structure contains the output VIO and output buffer. If the
   * private data structure pointer is NULL, then we'll create it
   * and initialize its internals.
   */
  data = TSContDataGet(contp);
  if (!data->output_buffer) {
    data->output_buffer = TSIOBufferCreate();
    data->output_reader = TSIOBufferReaderAlloc(data->output_buffer);
    data->output_vio = TSVConnWrite(output_conn, contp, data->output_reader, INT64_MAX);
#ifdef DEBUG
	data->i64_header_amount = TSVIONBytesGet(input_vio) * 0.02;
	data->l_starttime = myclock();
    TSDebug("throttle", "i64_header_amount %lld bytes f_rate [%f]", data->i64_header_amount, data->f_rate);
#endif
    // data->output_vio = TSVConnWrite(output_conn, contp, data->output_reader, TSVIONBytesGet(input_vio));
    TSContDataSet(contp, data);
  }

  /* We also check to see if the input VIO's buffer is non-NULL. A
   * NULL buffer indicates that the write operation has been
   * shutdown and that the upstream continuation does not want us to send any
   * more WRITE_READY or WRITE_COMPLETE events. For this simplistic
   * transformation that means we're done. In a more complex
   * transformation we might have to finish writing the transformed
   * data to our output connection.
   */
  buf_test = TSVIOBufferGet(input_vio);

  if (!buf_test) {
    TSVIONBytesSet(data->output_vio, TSVIONDoneGet(input_vio));
    TSVIOReenable(data->output_vio);
    return;
  }

  /* Determine how much data we have left to read. For this null
   * transform plugin this is also the amount of data we have left
   * to write to the output connection.
   */
  towrite = TSVIONTodoGet(input_vio);
  //TSDebug("throttle", "\ttoWrite is %" PRId64 "", towrite);

  if (towrite > 0) {
    /* The amount of data left to read needs to be truncated by
     * the amount of data actually in the read buffer.
     */
    avail = TSIOBufferReaderAvail(TSVIOReaderGet(input_vio));
	if (towrite > avail) {
	  if (avail > data->f_rate)
		  towrite = data->f_rate;
	  else
		  towrite = avail;
	}

	if (towrite > 0) {
		/* Copy the data from the read buffer to the output buffer. */
		TSIOBufferCopy(TSVIOBufferGet(data->output_vio), TSVIOReaderGet(input_vio), towrite, 0);

		/* Tell the read buffer that we have read the data and are no
		 * longer interested in it.
		 */
		TSIOBufferReaderConsume(TSVIOReaderGet(input_vio), towrite);
#ifdef DEBUG
		data->i64_total_sent += towrite;
#endif

		/* Modify the input VIO to reflect how much data we've
		 * completed.
		 */
		TSVIONDoneSet(input_vio, TSVIONDoneGet(input_vio) + towrite);
	}
  }

  /* Now we check the input VIO to see if there is data left to
   * read.
   */
  if (TSVIONTodoGet(input_vio) > 0) {
    if (towrite > 0) {
      /* If there is data left to read, then we reenable the output
       * connection by reenabling the output VIO. This will wake up
       * the output connection and allow it to consume data from the
       * output buffer.
       */
      TSVIOReenable(data->output_vio);

      /* Call back the input VIO continuation to let it know that we
       * are ready for more data.
       */
      TSContCall(TSVIOContGet(input_vio), TS_EVENT_VCONN_WRITE_READY, input_vio);
    }
  } else {
    /* If there is no data left to read, then we modify the output
     * VIO to reflect how much data the output connection should
     * expect. This allows the output connection to know when it
     * is done reading. We then reenable the output connection so
     * that it can consume the data we just gave it.
     */
    TSVIONBytesSet(data->output_vio, TSVIONDoneGet(input_vio));
    TSVIOReenable(data->output_vio);

    /* Call back the input VIO continuation to let it know that we
     * have completed the write operation.
     */
    TSContCall(TSVIOContGet(input_vio), TS_EVENT_VCONN_WRITE_COMPLETE, input_vio);
  }
}

static int
throttle_transform(TSCont contp, TSEvent event, void *edata)
{
	  struct timespec sleepTime; 
	  struct timespec time_left_to_sleep; 
	  sleepTime.tv_sec = 0; 
	  sleepTime.tv_nsec = 10000000; // 0.01 second 10 mili seconds.
	  time_left_to_sleep.tv_sec = 0;
	  time_left_to_sleep.tv_nsec = 0;
  /* Check to see if the transformation has been closed by a call to
   * TSVConnClose.
   */

  if (TSVConnClosedGet(contp)) {
    my_data_destroy(TSContDataGet(contp));
    TSContDestroy(contp);
    return 0;
  } 
  else {
    switch (event) {
    case TS_EVENT_ERROR:
      {
        TSVIO input_vio;

        TSDebug("throttle", "\tEvent is TS_EVENT_ERROR");
        /* Get the write VIO for the write operation that was
         * performed on ourself. This VIO contains the continuation of
         * our parent transformation. This is the input VIO.
         */
        input_vio = TSVConnWriteVIOGet(contp);

        /* Call back the write VIO continuation to let it know that we
         * have completed the write operation.
         */
        TSContCall(TSVIOContGet(input_vio), TS_EVENT_ERROR, input_vio);
      }
      break;
    case TS_EVENT_VCONN_WRITE_COMPLETE:
      /* When our output connection says that it has finished
       * reading all the data we've written to it then we should
       * shutdown the write portion of its connection to
       * indicate that we don't want to hear about it anymore.
       */
      TSVConnShutdown(TSTransformOutputVConnGet(contp), 0, 1);
      break;
    case TS_EVENT_VCONN_WRITE_READY:
		// simple throttle
		 nanosleep(&sleepTime, &time_left_to_sleep);

    default:
      /* If we get a WRITE_READY event or any other type of
       * event (sent, perhaps, because we were reenabled) then
       * we'll attempt to transform more data.
       */

      handle_transform(contp);
      break;
    }
  }

  return 0;
}

static int
transformable(TSHttpTxn txnp)
{
  /*
   *  We are only interested in transforming "200 OK" responses.
   */

  TSMBuffer bufp;
  TSMLoc hdr_loc;
  TSHttpStatus resp_status;
  int retv;


  TSHttpTxnServerRespGet(txnp, &bufp, &hdr_loc);
  resp_status = TSHttpHdrStatusGet(bufp, hdr_loc);
  retv = (resp_status == TS_HTTP_STATUS_OK);

  if (TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc) == TS_ERROR) {
    TSError("throttle Error releasing MLOC while checking " "header status\n");
  }

  return retv;
}


int get_param_value(char *dest, const char *src)
{
	int i_length = 0;

	if (!src) return 0;

	while (*src != '&' && *src != '\0' && *src != ' ') {
		i_length++;
		*dest++ = *src++;
	}

	return i_length;
}

static void
transform_add(TSHttpTxn txnp, int i_bitrate, TSCont contp)
{
	TSVConn connp;
	MyData *data;

	//TSDebug("throttle", "Entering transform_add()");
	connp = TSTransformCreate(throttle_transform, txnp);

	// Set the cache flag so that we search for cache object.
	data = my_data_alloc();

	// send data every 10 miliseconds(1/100 second). That's why i divide f_rate by 100.
	// but nanosleep is not very accurate in measurement of time. 
	// usually it returns after more time(110~120%) than i expected.
	// if a user want me to send 1Mbps i'd better send 1.2Mbps to assure 1Mbps in any temparary network problem.
	// so final correction that was determined by some experiments is to multiply f_rate by 1.6.
	// The unit is bps not Byte per second.
	data->f_rate = ((i_bitrate / 8) /100) * 1.6;

	TSContDataSet(connp, data);
	//TSHttpTxnHookAdd(txnp, TS_HTTP_READ_RESPONSE_HDR_HOOK, contp);
	TSHttpTxnHookAdd(txnp, TS_HTTP_RESPONSE_TRANSFORM_HOOK, connp);
}

static int 
cache_hit_check(TSHttpTxn txnp)
{
	int cache_check;
	TSHttpTxnCacheLookupStatusGet(txnp, &cache_check);
	switch(cache_check) {
	case TS_CACHE_LOOKUP_MISS:
		TSDebug("throttle", "TS_CACHE_LOOKUP_MISS");
		break;
	case TS_CACHE_LOOKUP_HIT_STALE :
		break;		
	case TS_CACHE_LOOKUP_HIT_FRESH :
		TSDebug("throttle", "TS_CACHE_LOOKUP_HIT_FRESH");
		break;		
	case TS_CACHE_LOOKUP_SKIPPED :
		break;		
	default:
		break;
	}

	return cache_check;
}

static int
check_bitrate_query(TSHttpTxn txnp, int *i_bitrate)
{
	TSMBuffer bufp;
	TSMLoc hdr_loc;
	TSMLoc url_loc;
	TSMLoc query_loc;
#ifdef DEBUG
	char *url_str;
	int url_length;
#endif
	const char *query = NULL;
	char *curr_pos;
	char param_value[256];  // check if 1024 byte is enoough!!
	char modified_query[1024];
	int query_length = 0;
	int	i_length;

	memset(param_value, 0x00, 256);
	if (TSHttpTxnClientReqGet(txnp, &bufp, &hdr_loc) != TS_SUCCESS) {
		TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
		TSError("can't retrieve client request header!\n");
		return 0;
	}

	if (TSHttpHdrUrlGet(bufp, hdr_loc, &query_loc) != TS_SUCCESS) {
		TSError("can't retrieve request url!\n");
		TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
		TSHandleMLocRelease(bufp, hdr_loc, query_loc);
		return 0;
	}

	TSHttpHdrUrlGet(bufp, hdr_loc, &url_loc);

	query = TSUrlHttpQueryGet(bufp, query_loc, &query_length);

#ifdef DEBUG
	url_str = TSUrlStringGet(bufp, url_loc, &url_length);

	TSDebug("throttle", "url is [%s] and length [%d] query [%s]", url_str, url_length, query);
#endif
	if (!query) {
		TSHandleMLocRelease(bufp, hdr_loc, url_loc);
		TSHandleMLocRelease(bufp, hdr_loc, query_loc);
		TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
		return 0;
	}
	else {
		curr_pos = strstr(query, "bitrate=");
		if (curr_pos) {
			i_length = get_param_value(param_value, curr_pos + 8);
			// TODO : if param1 should be number check it before memcpy by is_number(param_value);
			*i_bitrate = atof(param_value);
#ifdef DEBUG
	TSDebug("throttle", "bitrate from query string [%d]", *i_bitrate);
#endif
		}
		else {
			TSHandleMLocRelease(bufp, hdr_loc, url_loc);
			TSHandleMLocRelease(bufp, hdr_loc, query_loc);
			TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
			return 0;
		}
		
		if ((curr_pos - query) == 0) {
			if (TSUrlHttpQuerySet(bufp, url_loc, NULL, -1) != TS_SUCCESS)
				TSError("throttle", "error in QuerySet");
		}
		else {
			memset(modified_query, 0x00, sizeof(modified_query));
			memcpy(modified_query, query, curr_pos - query - 1);
			if (TSUrlHttpQuerySet(bufp, url_loc, modified_query, -1) != TS_SUCCESS)
				TSError("throttle", "error in QuerySet");
		}

#ifdef DEBUG
		query = TSUrlHttpQueryGet(bufp, query_loc, &query_length);
		if (!query) TSDebug("throttle", "remove bitrate is ok!!");
		TSHttpHdrUrlGet(bufp, hdr_loc, &url_loc);
		url_str = TSUrlStringGet(bufp, url_loc, &url_length);
		TSDebug("throttle", "url is [%s] and length [%d]", url_str, url_length);
		if (url_str) TSfree(url_str);
#endif
		TSHandleMLocRelease(bufp, hdr_loc, url_loc);
		TSHandleMLocRelease(bufp, hdr_loc, query_loc);
		TSHandleMLocRelease(bufp, TS_NULL_MLOC, hdr_loc);
		return 1;
	}
}

static int
check_bitrate_config(TSHttpTxn txnp, int *i_bitrate)
{
	int i;
	char *url;
	int url_length;

	url = TSHttpTxnEffectiveUrlStringGet(txnp, &url_length);
	if (!url) {
		TSDebug("throttle", "get url error in check_bitrate_config");
		return 0;
	}

	for (i = 0; i < BPDCOUNT; i++) {
		if ((strncmp(url, bpd[i].domain, bpd[i].domain_length) == 0) || bpd[i].domain == NULL) break;
	}
#ifdef DEBUG
	TSDebug("throttle", "url is [%s] length [%d]", url, url_length);
	TSDebug("throttle", "config get rate [%f]", bpd[i].f_rate);
#endif

	*i_bitrate = bpd[i].f_rate;

	if (url) TSfree(url);

	return 1;
}

static int 
get_bitrate(TSHttpTxn txnp)
{
	int i_bitrate;

	// First	step : seaarch bitrate from query
	// Second	step : search url from throttle config file
	
	if (check_bitrate_query(txnp, &i_bitrate)) {
#ifdef DEBUG
		TSDebug("throttle", "bitrate from query %d", i_bitrate);
#endif
		return i_bitrate;
	}
	else if (check_bitrate_config(txnp, &i_bitrate)) {
#ifdef DEBUG
		TSDebug("throttle", "bitrate from config %d", i_bitrate);
#endif
		return i_bitrate;
	}
	else {
		return 0;
	}
}

static int
throttle_transform_plugin(TSCont contp, TSEvent event, void *edata)
{
  TSHttpTxn txnp = (TSHttpTxn) edata;
  int i_bitrate = 0;

	//TSDebug("throttle", "Entering throttle_transform_plugin");
  switch (event) {
	case TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE :
		if (cache_hit_check(txnp) == TS_CACHE_LOOKUP_HIT_FRESH) {
			if ((i_bitrate = get_bitrate(txnp))) {
#ifdef DEBUG
				TSDebug("throttle", "got bitrate [%d] TS_EVENT_HTTP_CACHE_LOOKUP_COMPLETE", i_bitrate);
#endif
				transform_add(txnp, i_bitrate, contp);
			}
		}

		TSHttpTxnReenable(txnp, TS_EVENT_HTTP_CONTINUE);
		return 0;
	case TS_EVENT_HTTP_READ_RESPONSE_HDR :
		if (transformable(txnp)) {
			if ((i_bitrate = get_bitrate(txnp))) {
#ifdef DEBUG
				TSDebug("throttle", "got bitrate [%d] TS_EVENT_HTTP_READ_RESPONSE_HDR", i_bitrate);
#endif
				transform_add(txnp, i_bitrate, contp);
			}
		}
		TSHttpTxnReenable(txnp, TS_EVENT_HTTP_CONTINUE);
		return 0;
	default:
  		TSDebug("throttle", "unexpected event %d", event);
		break;
  }

  return 0;
}

int
check_ts_version()
{

  const char *ts_version = TSTrafficServerVersionGet();
  int result = 0;

  if (ts_version) {
    int major_ts_version = 0;
    int minor_ts_version = 0;
    int patch_ts_version = 0;

    if (sscanf(ts_version, "%d.%d.%d", &major_ts_version, &minor_ts_version, &patch_ts_version) != 3) {
      return 0;
    }

    /* Need at least TS 2.0 */
    if (major_ts_version >= 2) {
      result = 1;
    }

  }

  return result;
}

static void load_config_file()
{
    char buffer[1024];
    char config_file[1024];
    TSFile fh;

    /* locations in a config file line, end of line, split start, split end */
    char *eol, *spstart, *spend;
    int lineno = 0;
	int i;

    sprintf(config_file, "%s/throttle.config", THROTTLE_DIR);
    fh = TSfopen(config_file, "r");

    if (!fh) {
        TSError("[%s] Unable to open %s. No patterns will be loaded\n",
                "throttle", config_file);
        return;
    }

    while (TSfgets(fh, buffer, sizeof(buffer) - 1)) {
        if ( bpdcount > BPDCOUNT ) {
			TSDebug("throttle", "Warning, too many domains - skipping the rest (max: %d)", BPDCOUNT);
            break;
        }

        lineno++;
        if (*buffer == '#') {
            /* # Comments, only at line beginning */
            continue;
        }
        eol = strstr(buffer, "\n");
        if (eol) {
            *eol = 0; /* Terminate string at newline */
        } else {
            /* Malformed line - skip */
            continue;
        }
        /* Split line into two parts based on whitespace */
        /* Copy first part(domain) */
		for (i = 0; i < MAX_DOMAIN_LEN - 1; i++) {
			bpd[bpdcount].domain[i] = buffer[i];
			if (buffer[i] == ' ' || buffer[i] == '\t' ) {
				bpd[bpdcount].domain[i] = 0x00;
				bpd[bpdcount].domain_length = i;
				break;
			}
		}

        /* Find first whitespace and save second part(bitrate)*/
        spstart = strstr(buffer, " ");
        if (!spstart) {
            spstart = strstr(buffer, "\t");
        }
        if (!spstart) {
            TSError("[%s] ERROR: Invalid format on line %d. Skipping\n",
                    "throttle", lineno);
            continue;
        }
        /* Find part of the line after any whitespace */
        spend = spstart + 1;
        while(*spend == ' ' || *spend == '\t') {
            spend++;
        }
        if (*spend == 0) {
            /* We reached the end of the string without any non-whitepace */
            TSError("[%s] ERROR: Invalid format on line %d. Skipping\n",
                    "throttle", lineno);
            continue;
        }

        *spstart = 0;

        bpd[bpdcount].f_rate = atof(spend);
#ifdef DEBUG
        TSDebug("throttle", "Adding domain/bitrate pair: '%s -> '%f", bpd[bpdcount].domain, bpd[bpdcount].f_rate);
#endif

        bpdcount++;
    }
    TSfclose(fh);
}

void
TSPluginInit(int argc, const char *argv[])
{
  TSPluginRegistrationInfo info;

  info.plugin_name = "throttle";
  info.vendor_name = "nimbus networks";
  info.support_email = "jaekyung.oh@nimbusnetworks.co.kr";

  if (TSPluginRegister(TS_SDK_VERSION_3_0, &info) != TS_SUCCESS) {
    TSError("throttle Plugin registration failed.\n");
    goto Lerror;
  }

  if (!check_ts_version()) {
    TSError("throttle Plugin requires Traffic Server 3.0 " "or later\n");
    goto Lerror;
  }

	load_config_file();

	// make a continuation
	TSHttpHookAdd(TS_HTTP_CACHE_LOOKUP_COMPLETE_HOOK, TSContCreate(throttle_transform_plugin, NULL));
	TSHttpHookAdd(TS_HTTP_READ_RESPONSE_HDR_HOOK, TSContCreate(throttle_transform_plugin, NULL));
	TSDebug("throttle", "initialize ok");

  return;

Lerror:
  TSError("throttle Unable to initialize plugin (disabled).\n");
}
