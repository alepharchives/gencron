-module (gen_expire_test).
-export ([ start_link/3, stop/1 ]).
-behaviour (gen_expire).
-export ([ activity_context/2,
           first/2,
           next/3,
           delete/3,
           finish/1,
           init/1,
           handle_call/3,
           handle_cast/2,
           handle_info/2,
           terminate/2,
           code_change/3 ]).

-record (mega, { expect }).

-include ("../src/gen_expire.hrl").

%-=====================================================================-
%-                                Public                               -
%-=====================================================================-

start_link (Interval, Limit, Tab) ->
  gen_expire:start_link (?MODULE, Interval, [ Limit, Tab ], []).

stop (ServerRef) ->
  gen_server:cast (ServerRef, stop).

%-=====================================================================-
%-                         gen_expire callbacks                        -
%-=====================================================================-

activity_context (_Table, State = #mega{ expect = Expect }) 
  when (Expect =:= activity_context) orelse 
       (Expect =:= finish) orelse
       (is_tuple (Expect) andalso element (1, Expect) =:= next) ->
  { sync_dirty, State#mega{ expect = first } }.

first (Table, State = #mega{ expect = first }) ->
  case mnesia:first (Table) of
    '$end_of_table' -> 
      { end_of_table, State#mega{ expect = finish } };
    Key ->
      { ok, Key, State#mega{ expect = { next, Table, Key } } }
  end.

next (Table, Key, State = #mega{ expect = { next, Table, Key } }) ->
  case mnesia:next (Table, Key) of
    '$end_of_table' -> 
      { end_of_table, State#mega{ expect = { delete, Table, Key } } };
    Next ->
      { ok, Next, State#mega{ expect = { delete, Table, Key } } }
  end.

delete (Table, Key, State = #mega{ expect = { delete, Table, Key } }) ->
  Expect = 
    case mnesia:next (Table, Key) of
      '$end_of_table' -> finish;
      Next -> { next, Table, Next }
    end,
  mnesia:delete (Table, Key, write),
  mnesia:delete (list_to_atom (atom_to_list (Table) ++ "_dup"), Key, write),
  { ok, State#mega{ expect = Expect } }.

finish (#mega{ expect = Expect }) when Expect =:= finish orelse 
                                       (is_tuple (Expect) andalso
                                        element (1, Expect) =:= next) ->
  timer:sleep (10).

init ([ Limit, Tab ]) ->
  { ok, 
    [ #expirespec{ table = Tab, max_bytes_per_box = Limit } ],
    #mega{ expect = activity_context } }.

handle_call (_Request, _From, State) -> { noreply, State }.
handle_cast (stop, State) -> { stop, normal, State };
handle_cast (_Request, State) -> { noreply, State }.
handle_info (_Msg, State) -> { noreply, State }.
terminate (_Reason, _State) -> ok.
code_change (_OldVsn, State, _Extra) -> { ok, State }.
