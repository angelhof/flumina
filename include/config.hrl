
%%%
%%% Global configuration and constants
%%%

-ifndef(LOG_DIR).
-define(LOG_DIR, "logs").
-endif.

-ifndef(SINK_WAITING_TIME_MS).
-define(SINK_WAITING_TIME_MS, 5000).
-endif.

-ifndef(ASYNC_MESSAGE_LOGGER_BUFFER_SIZE).
-define(ASYNC_MESSAGE_LOGGER_BUFFER_SIZE, 100).
-endif.

-ifndef(MBOX_BUFFER_SIZE_LIMIT).
-define(MBOX_BUFFER_SIZE_LIMIT, 10000).
-endif.

-ifndef(MBOX_BACKPRESSURE).
-define(MBOX_BACKPRESSURE, false).
-endif.

-ifndef(PROFILE).
-define(PROFILE, false).
-endif.

-ifndef(DEBUG).
-define(DEBUG, false).
-endif.

%% This time is not monotonic, however it can be used to synchronize
%% processes residing in different machines. Erlang monotonic time is
%% unique to one erlang vm and does not have anything to do with the
%% others.
%%
%% Consider changing that to erlang:system_time. Could this be more
%% efficient? Or have some other benefit?
-define(GET_SYSTEM_TIME(), os:system_time()). %% This is in nanoseconds
-define(GET_SYSTEM_TIME(TimeUnit), os:system_time(TimeUnit)).
