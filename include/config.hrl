
%%%
%%% Global configuration and constants
%%%

-define(LOG_DIR, "logs").

-define(SINK_WAITING_TIME_MS, 15000).

%% This time is not monotonic, however it can be used to synchronize
%% processes residing in different machines. Erlang monotonic time is
%% unique to one erlang vm and does not have anything to do with the
%% others.
%%
%% Consider changing that to erlang:system_time. Could this be more
%% efficient? Or have some other benefit?
-define(GET_SYSTEM_TIME(), os:system_time()). %% This is in nanoseconds
-define(GET_SYSTEM_TIME(TimeUnit), os:system_time(TimeUnit)).
