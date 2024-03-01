-module(assignment3).
-export([master/2, create_chord_ring/5, create_nodes/6, create_finger_table/6, is_present_in_list/4, calculate_hop/4, kill_PID/2, start_killing/1]).

is_present_in_list(Successor1, Successor2, Sorted_Hashkey_List, TotalNodes) ->
    In_list = lists:member(Successor1, Sorted_Hashkey_List),

    if (Successor1 -1) == Successor2 ->
        -999;
    In_list ->
        Successor1;
    Successor1 > Successor2 ->
        if Successor1 /= TotalNodes ->
            New_Successor1 = Successor1 + 1,
            is_present_in_list(New_Successor1, Successor2, Sorted_Hashkey_List, TotalNodes);
        Successor1 == TotalNodes ->
            New_Successor1 = 1,
            is_present_in_list(New_Successor1, Successor2, Sorted_Hashkey_List, TotalNodes)
        end;
    Successor1 =< Successor2 ->
        New_Successor1 = Successor1 + 1,
        is_present_in_list(New_Successor1, Successor2, Sorted_Hashkey_List, TotalNodes)
    end.

create_finger_table(_, _, _, 0, _, Finger_Table) ->
    %io:format("Finger Table for Hashkey ~w is ~w ~n. Sorted_Hashkey_List : ~w ;~n", [Hashkey, Finger_Table, Sorted_Hashkey_List]),
    %timer:sleep(10000),
    Finger_Table;

create_finger_table(Hashkey, Sorted_Hashkey_List, CurrentM, M, TotalM, Finger_Table) ->
    %io:format("Hashkey is ~w Sorted list is ~w ~n", [Hashkey, Sorted_Hashkey_List]),
    Index = string:str(Sorted_Hashkey_List, [Hashkey]),

    if (Index + 1) =< length(Sorted_Hashkey_List) ->
        Successor = lists:nth(Index + 1, Sorted_Hashkey_List);
    (Index + 1) > length(Sorted_Hashkey_List) ->
        Successor = lists:nth(1, Sorted_Hashkey_List)
    end,
    
    TotalNodes = round(math:pow(2, TotalM)),

    Successor1 = (Hashkey + round(math:pow(2, CurrentM))) rem TotalNodes,

    if CurrentM /= (TotalM -1) ->
        Successor2 = Hashkey + (round(math:pow(2, CurrentM+1))) -1;
    CurrentM == (TotalM -1) ->
        Successor2 = Hashkey -1
    end,
    
    %io:format("Outer Loop Here Successor1 ~w Successor2 ~w Sorted_Hashkey_List ~w TotalNodes ~w ~n", [Successor1, Successor2, Sorted_Hashkey_List, TotalNodes]),
    
    Is_Present_In_List = is_present_in_list(Successor1, Successor2, Sorted_Hashkey_List, TotalNodes),

    if Is_Present_In_List /= -999 ->
        New_Successor = Is_Present_In_List;
    Is_Present_In_List == -999 ->
        New_Successor = Successor
    end,

    New_Finger_Table = Finger_Table ++ [New_Successor],
    %io:format("M is ~w Finger Table for Hashkey ~w is ~w ~n", [M, Hashkey, Finger_Table]),
    create_finger_table(Hashkey, Sorted_Hashkey_List, (CurrentM+1), (M-1), TotalM, New_Finger_Table).

initiate_msg(0, _, _, _) ->
    ok;

initiate_msg(Num_Of_Requests, Hashkey, Finger_Table, M) ->
    Hop_Counter = counters:new(1, [atomics]),
    %Select_Msg = rand:uniform(round(math:pow(2, M))),
    Select_Msg = binary_to_integer(binary:encode_hex(crypto:hash(sha256, crypto:strong_rand_bytes(10))), 16) rem round(math:pow(2, M)) +1, 
    
    send_msg(Hop_Counter, Hashkey, Finger_Table, Select_Msg, M),
    initiate_msg(Num_Of_Requests -1, Hashkey, Finger_Table, M).

send_msg(Hop_Counter, Hashkey, Finger_Table, Select_Msg, M) ->

    %io:format("Select_Msg ~w; Hashkey ~w; M ~w;", [Select_Msg, Hashkey, M]),
    Position = get_position(Select_Msg, Hashkey, M, M, 0),
    %io:format("Position is ~w; Finger_Table ~w; ~n", [Position, Finger_Table]),
    Select_Hashkey = lists:nth(Position, Finger_Table),
    
    if (Hashkey < Select_Hashkey) and (Select_Msg > Select_Hashkey)->
        counters:add(Hop_Counter, 1, 1),
        Next_PID = whereis(list_to_atom("pid" ++ integer_to_list(Select_Hashkey))),
        Next_PID ! [fingertable, Hop_Counter, Select_Hashkey, Select_Msg, M];
    (Hashkey < Select_Hashkey) and (Select_Msg =< Select_Hashkey)->
        counters:add(Hop_Counter, 1, 1),
        %io:format("Hop_Counter is ~w ~n", [counters:get(Hop_Counter, 1)]),
        hop_counter ! [calculatehop, counters:get(Hop_Counter, 1)];
    (Hashkey > Select_Hashkey)->
        counters:add(Hop_Counter, 1, 1),
        %io:format("Hop_Counter is ~w ~n", [counters:get(Hop_Counter, 1)]),
        hop_counter ! [calculatehop, counters:get(Hop_Counter, 1)];
    true ->
        io:format("Select Msg is ~w; Select Hashkey is ~w; Hashkey : ~w; Hop_Counter ~w; ~n", [Select_Msg, Select_Hashkey, Hashkey, counters:get(Hop_Counter, 1)])
    end.

calculate_hop(Counter, Running_Count, Total_Count, Master_PID) ->
    receive
        [calculatehop, Hop_Count] ->
            New_Counter = Counter + Hop_Count,
            %io:format("~w Hop received; ~w Total Hop ~n", [Hop_Count, New_Counter]),

            Convergence = Running_Count == Total_Count,
            case Convergence of
                true ->
                    Master_PID ! [finish, New_Counter];
                false ->
                    %io:format("False Convergence", []),
                    calculate_hop(New_Counter, Running_Count+1, Total_Count, Master_PID)
            end
    end.

get_position(_, _, 0, _, _) ->
    errorMnotfound;

get_position(Select_Msg, Hashkey, M, TotalM, CurrentM) ->

    TotalNodes = round(math:pow(2, TotalM)),
    Successor1 = (Hashkey + round(math:pow(2, CurrentM))) rem TotalNodes,

    if CurrentM /= (TotalM -1) ->
        Successor2 = Hashkey + (round(math:pow(2, CurrentM+1))) -1;
    CurrentM == (TotalM -1) ->
        Successor2 = Hashkey -1
    end,

    %io:format("Successor1 ~w; Successor2 ~w; ~n", [Successor1, Successor2]),

    if (CurrentM /= (TotalM -1)) and (Select_Msg >= Successor1) and (Select_Msg =< Successor2) ->
        (CurrentM +1);
    (CurrentM == (TotalM -1)) and (((Select_Msg >= Successor1) and (Select_Msg =< TotalNodes)) or ((Select_Msg >= 1) and (Select_Msg =< Successor2))) ->
        (CurrentM +1);
    true ->
        get_position(Select_Msg, Hashkey, M-1, TotalM, CurrentM+1)
    end.

create_nodes(Num_Of_Nodes, Num_Of_Requests, Sorted_Hashkey_List, M, Index, Finger_Table) ->
    Hashkey = binary_to_integer(binary:encode_hex(crypto:hash(sha256, integer_to_list(Num_Of_Nodes))), 16) rem round(math:pow(2, M)) + 1, 

    %Hash = list_to_binary(lists:sublist(binary_to_list(crypto:hash(sha, integer_to_list(Num_Of_Nodes))), 2)),
    %Hashkey = binary_to_integer(binary:encode_hex(Hash), 16) rem round(math:pow(2, M)) + 1, 
    %io:format("Hash : ~w ; Hashkey : ~w ; Before Rem : ~w ;~n", [Hash, Hashkey, binary_to_integer(binary:encode_hex(Hash), 16)]),

    if (Index == 1) ->
        register(list_to_atom("pid" ++ integer_to_list(Hashkey)), self());
        %io:format("PID : ~w; Registered to : ~w; ~n", [self(), Hashkey]),
    true ->
        ok
    end,

    if (Index == 1) ->
        New_Finger_Table = create_finger_table(Hashkey, Sorted_Hashkey_List, 0, M, M, [] );
    true ->
        New_Finger_Table = Finger_Table
    end,

    if (Index == 1) ->
        timer:sleep(10000),
        initiate_msg(Num_Of_Requests, Hashkey, New_Finger_Table, M);
    true ->
        ok
    end,
    
    receive 
        [fingertable, Hop_Counter, Hashkey, Select_Msg, M] ->
            send_msg(Hop_Counter, Hashkey, New_Finger_Table, Select_Msg, M)    
    end,
    create_nodes(Num_Of_Nodes, Num_Of_Requests, Sorted_Hashkey_List, M, Index+1, New_Finger_Table).

start_killing(PID_List) ->
    receive
        kill ->
            kill_PID(PID_List, length(PID_List))
    end.

kill_PID(_, 0) ->
    ok;

kill_PID(PID_List, Len) ->
    %io:format("Killing all", []),
    PID = lists:nth(Len, PID_List),
    exit(PID, kill),
    kill_PID(PID_List, Len-1).

create_chord_ring(0, _, PID_List, _, _) ->
    %io:format("PIDList : ~w;~n ", [PID_List]),
    %io:format("Sorted Hashkey_List : ~w;~n ", [Sorted_Hashkey_List]),
    Start_killing_PID = spawn(assignment3, start_killing, [PID_List]),
    receive
        kill ->
            Start_killing_PID ! kill
    end;

create_chord_ring(Num_Of_Nodes, Num_Of_Requests, PID_List, Sorted_Hashkey_List, M) ->
    PID = spawn_link(assignment3, create_nodes, [Num_Of_Nodes, Num_Of_Requests, Sorted_Hashkey_List, M, 1, []]),
    %io:format("PID : ~w, Register : ~s ; ", [PID, "pid" ++ integer_to_list(Num_Of_Nodes)]),

    New_PID_List = PID_List ++ [PID],

    create_chord_ring(Num_Of_Nodes - 1, Num_Of_Requests, New_PID_List, Sorted_Hashkey_List, M).

create_hashkey_list(Hashkey_List, 0, _) ->
    Hashkey_List;

create_hashkey_list(Hashkey_List, Num_Of_Nodes, M) ->
    %io:format("In create hashkey list ", []),
    Hashkey = binary_to_integer(binary:encode_hex(crypto:hash(sha256, integer_to_list(Num_Of_Nodes))), 16) rem round(math:pow(2, M)) + 1, 
    
    %Hash = list_to_binary(lists:sublist(binary_to_list(crypto:hash(sha, integer_to_list(Num_Of_Nodes))), 2)),
    %Hashkey = binary_to_integer(binary:encode_hex(Hash), 16) rem round(math:pow(2, M)) + 1, 
    %io:format("Hash : ~w ; Hashkey : ~w ; Before Rem : ~w ;~n", [Hash, Hashkey, binary_to_integer(binary:encode_hex(Hash), 16)]),

    New_Hashkey_List = Hashkey_List ++ [Hashkey],
    create_hashkey_list(New_Hashkey_List, Num_Of_Nodes -1, M).

master(Num_Of_Nodes, Num_Of_Requests) ->
    M = 26,
    Hashkey_List = create_hashkey_list([], Num_Of_Nodes, M),
    Sorted_Hashkey_List = lists:sort(Hashkey_List),
    Total_Msg = (Num_Of_Nodes * Num_Of_Requests),

    Hop_Counter_PID = spawn_link(assignment3, calculate_hop, [0, 1, Total_Msg, self()]),
    register(hop_counter, Hop_Counter_PID),
    Chord_Ring_PID = spawn(assignment3, create_chord_ring, [Num_Of_Nodes, Num_Of_Requests, [], Sorted_Hashkey_List, M]),

    receive
        [finish, New_Counter] ->
            Avg_Hop = New_Counter / Total_Msg,
            Chord_Ring_PID ! kill,
            io:format("All the messages have been sent. The avg number of hops with ~w nodes and ~w msg requests is ~w.~n", [Num_Of_Nodes, Num_Of_Requests, Avg_Hop])
    end.



