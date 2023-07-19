import io
import os
import zstandard as zstd
from pathlib import Path
import psycopg
from psycopg import sql
import time


file = Path("C:\\Users\\JoeH\\Downloads\\lichess_db_standard_rated_2023-05.pgn.zst")
DCTX = zstd.ZstdDecompressor(max_window_size=2**31)

# Connect to your postgres DB
conn = psycopg.connect("dbname=test user=JoeH password=a")
# # Open a cursor to perform database operations
cur = conn.cursor()


def tables():
    meta_dict = {
        "game_id": "bigint",
        "rated": "boolean",
        "bullet": "boolean",
        "blitz": "boolean",
        "rapid": "boolean",
        "classical": "boolean",
        "correspond": "boolean",
        "event": "text",
        "white_won": "boolean",
        "draw": "boolean",
        "white_elo": "float",
        "black_elo": "float",
        "white_rating_diff": "float",
        "black_rating_diff": "float",
        "eco": "char(3)",
        "opening": "text",
        "time_control": "float",
        "termination": "text",
        "increment": "boolean",
        "increment_amount": "float",
        "site": "text",
    }

    result = ", ".join([f"{key} {value}" for key, value in meta_dict.items()])

    drop_statement1 = sql.SQL("DROP TABLE IF EXISTS metadata;")
    create_statement1 = sql.SQL(
        f"""CREATE TABLE metadata
                ({result}) TABLESPACE usb;"""
    )
    unlog_statement1 = sql.SQL("ALTER TABLE metadata SET UNLOGGED;")
    cur.execute(drop_statement1)
    cur.execute(create_statement1)
    cur.execute(unlog_statement1)

    game_dict = {
        "length": "float",
        "game_number_of_blunders": "float",
        "number_of_white_blunders": "float",
        "number_of_black_blunders": "float",
        "game_number_of_mistakes": "float",
        "number_of_white_mistakes": "float",
        "number_of_black_mistakes": "float",
        "game_number_of_inaccuracies": "float",
        "number_of_white_inaccuracies": "float",
        "number_of_black_inaccuracies": "float",
        "has_evals": "boolean",
        "evals": "text[]",
        "has_annotations": "boolean",
        "game_checks": "float",
        "game_black_checks": "float",
        "game_white_checks": "float",
        "num_white_checks": "float",
        "game_captures": "float",
        "game_white_captures": "float",
        "game_black_captures": "float",
        "game_promotions": "float",
        "game_black_promotions": "float",
        "game_black_q_promotions": "float",
        "game_black_n_promotions": "float",
        "game_black_b_promotions": "float",
        "game_black_r_promotions": "float",
        "game_white_promotions": "float",
        "game_white_q_promotions": "float",
        "game_white_n_promotions": "float",
        "game_white_b_promotions": "float",
        "game_white_r_promotions": "float",
        "potentially_ambigous_moves": "float",
        "game_black_q_castle": "boolean",
        "game_black_k_castle": "boolean",
        "game_white_q_castle": "boolean",
        "game_white_k_castle": "boolean",
        "game_id": "bigint",
    }

    result = ", ".join([f"{key} {value}" for key, value in game_dict.items()])

    drop_statement2 = sql.SQL("DROP TABLE IF EXISTS game_data;")
    create_statement2 = sql.SQL(
        f"""CREATE TABLE game_data
                ({result}) TABLESPACE usb;"""
    )
    unlog_statement2 = sql.SQL("ALTER TABLE game_data SET UNLOGGED;")
    cur.execute(drop_statement2)
    cur.execute(create_statement2)
    cur.execute(unlog_statement2)

    white_dict = {
        "game_id": "bigint",
        "timestamps_white": "float[]",
        "game_white_piece_data": "text[]",
        "game_white_location_data": "text[]",
        "game_white_move_num_data": "float[]",
        "game_white_captures_data": "boolean[]",
        "game_white_castle_data": "boolean[]",
        "game_white_checks_data": "boolean[]",
        "game_white_promotions_data": "boolean[]",
    }

    result = ", ".join([f"{key} {value}" for key, value in white_dict.items()])

    drop_statement3 = sql.SQL("DROP TABLE IF EXISTS white_data;")
    create_statement3 = sql.SQL(
        f"""CREATE TABLE white_data
            ({result}) TABLESPACE usb;"""
    )
    unlog_statement3 = sql.SQL("ALTER TABLE white_data SET UNLOGGED;")
    cur.execute(drop_statement3)
    cur.execute(create_statement3)
    cur.execute(unlog_statement3)

    black_dict = {
        "game_id": "bigint",
        "timestamps_black": "float[]",
        "game_black_piece_data": "text[]",
        "game_black_location_data": "text[]",
        "game_black_move_num_data": "float[]",
        "game_black_captures_data": "boolean[]",
        "game_black_castle_data": "boolean[]",
        "game_black_checks_data": "boolean[]",
        "game_black_promotions_data": "boolean[]",
    }

    result = ", ".join([f"{key} {value}" for key, value in black_dict.items()])

    drop_statement4 = sql.SQL("DROP TABLE IF EXISTS black_data;")
    create_statement4 = sql.SQL(
        f"""CREATE TABLE black_data
            ({result}) TABLESPACE usb;"""
    )
    unlog_statement4 = sql.SQL("ALTER TABLE black_data SET UNLOGGED;")
    cur.execute(drop_statement4)
    cur.execute(create_statement4)
    cur.execute(unlog_statement4)
    conn.commit()


def insert_into_db(full_parsed_game, current_counter_val, data_dict):
    num_of_entries_at_once = 1000
    counter_val = current_counter_val % num_of_entries_at_once

    dict_meta_copy = full_parsed_game["metadata"].copy()
    dict_game_copy = full_parsed_game["game_data"].copy()
    del dict_game_copy["long_data"]
    dict_white_copy = full_parsed_game["game_data"]["long_data"]["white"].copy()
    dict_black_copy = full_parsed_game["game_data"]["long_data"]["black"].copy()

    data_dict["meta_list"].append(dict_meta_copy)
    data_dict["game_list"].append(dict_game_copy)
    data_dict["white_list"].append(dict_white_copy)
    data_dict["black_list"].append(dict_black_copy)

    if counter_val == 999:
        meta_columns = dict_meta_copy.keys()
        meta_sql = ", ".join(meta_columns)
        meta_sql = "(" + meta_sql + ")"

        game_columns = dict_game_copy.keys()
        game_sql = ", ".join(game_columns)
        game_sql = "(" + game_sql + ")"

        white_columns = dict_white_copy.keys()
        white_sql = ", ".join(white_columns)
        white_sql = "(" + white_sql + ")"

        black_columns = dict_black_copy.keys()
        black_sql = ", ".join(black_columns)
        black_sql = "(" + black_sql + ")"

        meta_vals = [
            tuple(
                d.values(),
            )
            for d in data_dict["meta_list"]
        ]

        game_vals = [
            tuple(
                d.values(),
            )
            for d in data_dict["game_list"]
        ]

        white_vals = [
            tuple(
                d.values(),
            )
            for d in data_dict["white_list"]
        ]

        black_vals = [
            tuple(
                d.values(),
            )
            for d in data_dict["black_list"]
        ]

        data_dict["meta_list"] = []
        data_dict["game_list"] = []
        data_dict["white_list"] = []
        data_dict["black_list"] = []

        meta_statement = "COPY metadata " + str(meta_sql) + " FROM STDIN"

        with cur.copy(meta_statement) as meta_copy:
            for meta_record in meta_vals:
                meta_copy.write_row(meta_record)
        conn.commit()

        game_statement = "COPY game_data " + str(game_sql) + " FROM STDIN"

        with cur.copy(game_statement) as game_copy:
            for game_record in game_vals:
                game_copy.write_row(game_record)
        conn.commit()

        white_statement = "COPY white_data " + str(white_sql) + " FROM STDIN"

        with cur.copy(white_statement) as white_copy:
            for white_record in white_vals:
                white_copy.write_row(white_record)
        conn.commit()

        black_statement = "COPY black_data " + str(black_sql) + " FROM STDIN"

        with cur.copy(black_statement) as black_copy:
            for black_record in black_vals:
                black_copy.write_row(black_record)
        conn.commit()


def read_lines_from_zst_file(zstd_file_path: Path):
    with (
        zstd.open(zstd_file_path, mode="rb", dctx=DCTX) as zfh,
        io.TextIOWrapper(zfh) as iofh,
    ):
        for line in iofh:
            yield line


def def_fields():
    metadata = {
        "game_id": 0,
        "rated": True,
        "bullet": False,
        "blitz": True,
        "rapid": False,
        "classical": False,
        "correspond": False,
        "event": "",
        "white_won": True,
        "draw": False,
        "white_elo": -1,
        "black_elo": -1,
        "white_rating_diff": -1,
        "black_rating_diff": -1,
        "eco": "Z99",
        "opening": "Unknown",
        "time_control": -100,
        "termination": "KO",
        "increment": False,
        "increment_amount": -1,
        "site": "",
    }

    white_dat = {
        "timestamps_white": [],
        "game_white_piece_data": [],
        "game_white_location_data": [],
        "game_white_move_num_data": [],
        "game_white_captures_data": [],
        "game_white_castle_data": [],
        "game_white_checks_data": [],
        "game_white_promotions_data": [],
        "game_id": 0,
    }

    black_dat = {
        "timestamps_black": [],
        "game_black_piece_data": [],
        "game_black_location_data": [],
        "game_black_move_num_data": [],
        "game_black_captures_data": [],
        "game_black_castle_data": [],
        "game_black_checks_data": [],
        "game_black_promotions_data": [],
        "game_id": 0,
    }

    game_str = {
        "game": "",
        "game_id": 0,
    }

    long_data = {
        "white": white_dat,
        "black": black_dat,
        "game_str": game_str,
    }

    game_data = {
        "evals": [],
        "length": 0,
        "game_number_of_blunders": 0,
        "number_of_white_blunders": 0,
        "number_of_black_blunders": 0,
        "game_number_of_mistakes": 0,
        "number_of_white_mistakes": 0,
        "number_of_black_mistakes": 0,
        "game_number_of_inaccuracies": 0,
        "number_of_white_inaccuracies": 0,
        "number_of_black_inaccuracies": 0,
        "has_evals": False,
        "has_annotations": False,
        "game_checks": 0,
        "game_black_checks": 0,
        "game_white_checks": 0,
        "num_white_checks": 0,
        "game_captures": 0,
        "game_white_captures": 0,
        "game_black_captures": 0,
        "game_promotions": 0,
        "game_black_promotions": 0,
        "game_black_q_promotions": 0,
        "game_black_n_promotions": 0,
        "game_black_b_promotions": 0,
        "game_black_r_promotions": 0,
        "game_white_promotions": 0,
        "game_white_q_promotions": 0,
        "game_white_n_promotions": 0,
        "game_white_b_promotions": 0,
        "game_white_r_promotions": 0,
        "potentially_ambigous_moves": 0,
        "game_black_q_castle": False,
        "game_black_k_castle": False,
        "game_white_q_castle": False,
        "game_white_k_castle": False,
        "game_id": 0,
        "long_data": long_data,
    }
    return {"metadata": metadata, "game_data": game_data}


def record_when_events(color, move, g, move_num):
    if "=" in move:
        g["game_" + color + "_promotions"] += 1
        g["game_promotions"] += 1
        g["long_data"][color]["game_" + color + "_promotions_data"].append(True)
        if "=Q" in move:
            g["game_" + color + "_q_promotions"] += 1
            # creates new string, but I think is worth, so we don't have to deal with edge cases
            move = move.replace("=Q", "")
        if "=B" in move:
            g["game_" + color + "_b_promotions"] += 1
            move = move.replace("=B", "")
        elif "=N" in move:
            g["game_" + color + "_n_promotions"] += 1
            move = move.replace("=N", "")
        elif "=R" in move:
            g["game_" + color + "_r_promotions"] += 1
            move = move.replace("=R", "")
    else:
        g["long_data"][color]["game_" + color + "_promotions_data"].append(False)

    if "+" in move:
        g["game_" + color + "_checks"] += 1
        g["game_checks"] += 1
        move = move.replace("+", "")
        g["long_data"][color]["game_" + color + "_checks_data"].append(True)
    else:
        g["long_data"][color]["game_" + color + "_checks_data"].append(False)

    if "x" in move:
        g["game_" + color + "_captures"] += 1
        g["game_captures"] += 1
        g["long_data"][color]["game_" + color + "_captures_data"].append(True)
        move = move.replace("x", "")
    else:
        g["long_data"][color]["game_" + color + "_captures_data"].append(False)

    if "!!" in move:
        move = move.replace("!!", "")
        g["has_annotations"] = True

    elif "!?" in move:
        move = move.replace("!?", "")
        g["has_annotations"] = True

    elif "?!" in move:
        move = move.replace("?!", "")
        g["has_annotations"] = True

    elif "!" in move:
        move = move.replace("!", "")
        g["has_annotations"] = True

    elif "?" in move:
        move = move.replace("?", "")
        g["has_annotations"] = True

    elif "#" in move:
        move = move.replace("#", "")

    return move


def piece_ambiguator(str, g, color, move_num, dest, move):
    g["long_data"][color]["game_" + color + "_piece_data"].append(str)
    if len(move) == 4:
        g["potentially_ambigous_moves"] += 1
        move = move[:1] + move[2:]
    g["long_data"][color]["game_" + color + "_location_data"].append(dest)


def record_when_and_where_pieces(color, move, g, move_num):
    if "O-O-O" in move:
        dest = "O-O-O"
        g["game_" + color + "_q_castle"] = True
        g["long_data"][color]["game_" + color + "_piece_data"].append(dest)
        g["long_data"][color]["game_" + color + "_location_data"].append(dest)
        g["long_data"][color]["game_" + color + "_castle_data"].append(True)
    elif "O-O" in move:
        dest = "O-O"
        g["game_" + color + "_k_castle"] = True
        g["long_data"][color]["game_" + color + "_piece_data"].append(dest)
        g["long_data"][color]["game_" + color + "_location_data"].append(dest)
        g["long_data"][color]["game_" + color + "_castle_data"].append(True)
    else:
        g["long_data"][color]["game_" + color + "_castle_data"].append(False)
        dest = move[-2:]

        if move[0] == "N":
            piece_ambiguator("N", g, color, move_num, dest, move)
        elif move[0] == "B":
            piece_ambiguator("B", g, color, move_num, dest, move)
        elif move[0] == "Q":
            piece_ambiguator("Q", g, color, move_num, dest, move)
        elif move[0] == "K":
            piece_ambiguator("K", g, color, move_num, dest, move)
        elif move[0] == "R":
            piece_ambiguator("R", g, color, move_num, dest, move)
        else:
            g["long_data"][color]["game_" + color + "_piece_data"].append("P")
            if len(move) == 3:
                g["potentially_ambigous_moves"] += 1
            g["long_data"][color]["game_" + color + "_location_data"].append(dest)


def annotations_and_timings(color, split_move, g):
    # left index
    # NOTE: I want this to error with rindex, so that it can be caught in the except clause
    li = split_move[2].rindex("k")
    time = split_move[2][li + 2 : -2]
    g["long_data"][color]["timestamps_" + color].append(
        int(time[0]) * 3600 + int(time[2:4]) * 60 + int(time[5:])
    )
    l2i = split_move[2].find("eval")
    if l2i != -1:
        r2i = split_move[2].find("]")
        g["evals"].append(
            split_move[2][l2i + 5 : r2i]
        )  # can't be a float because of the M7. +5 bc of eval
        g["has_evals"] = True


def game_parser(game_data, g, fil):  # g is our fields OF GAME
    if "eval" in game_data[:20]:
        g["has_evals"] = True

    split_game_data = game_data.split("}")
    move_num = 0
    for move_data in split_game_data:
        move_num += 1
        g["length"] = move_num
        split_move = move_data.partition("{")
        split_move_notation = split_move[0]
        # if uses ... notation, then it is black to move
        if (split_move_notation.rfind(".") != -1) and (
            split_move_notation.rfind(".")
        ) == split_move_notation.find("."):
            from_index = split_move_notation.rindex(".")
            # Find the last dot in the move
            move = split_move_notation[(from_index + 2) : -1]
            # Shouldn't be any whitespace
            g["long_data"]["white"]["game_white_move_num_data"].append(move_num)

            try:
                move = record_when_events("white", move, g, move_num)
                record_when_and_where_pieces("white", move, g, move_num)
                annotations_and_timings("white", split_move, g)
            except:
                fil.write("we had an error parsing here\n")
                fil.write(game_data)
                return True
        # check this before black moves for the case of 1.e4 1-0
        elif (
            ("0-1" in split_move_notation)
            or ("1/2-1/2" in split_move_notation)
            or ("1-0" in split_move_notation)
        ):
            break
        else:
            from_index = split_move_notation.rindex(".")
            # Find the last dot in the move
            move = split_move_notation[(from_index + 2) : -1]
            # Shouldn't be any whitespace
            g["long_data"]["black"]["game_black_move_num_data"].append(move_num)

            try:
                move = record_when_events("black", move, g, move_num)
                record_when_and_where_pieces("black", move, g, move_num)
                annotations_and_timings("black", split_move, g)
            except:
                fil.write("we had a parsing error here \n")
                fil.write(game_data)
                True

    return False


def parse_game_metadata(debug, debug_idx, fil):
    fields = def_fields()
    counter = 1
    line_num = 0
    for line in read_lines_from_zst_file(file):
        line_num += 1
        if debug and (counter in debug_idx):
            print(
                f"extra debug statement, this is your counter, line_num, and line: {counter}, {line_num}\n{line}"
            )
            fil.write("\nextra debug statement, this is your counter and line: ")
            count = str(counter) + ", " + str(line_num)
            fil.write(count)
            lin = "\n" + str(line)
            fil.write(lin)
        if "Event" in line:
            fields = def_fields()
            if (
                ("Tournament" in line)
                or ("tournament" in line)
                or ("Arena" in line)
                or ("arena" in line)
            ):
                fields["metadata"]["event"] = line
        elif "Site" in line:
            fields["metadata"]["site"] = line.split('"')[1]
        if "Unrated" in line:
            fields["metadata"]["rated"] = False
        if "Correspondence" in line:
            fields["metadata"]["time_control"] = -1
            fields["metadata"]["increment_amount"] = -1
            fields["metadata"]["increment"] = False
            fields["metadata"]["correspond"] = True

        elif ("TimeControl" in line) and (fields["metadata"]["correspond"] == False):
            fields["metadata"]["time_control"] = float(line.split('"')[1].split("+")[0])
            fields["metadata"]["increment_amount"] = float(
                line.split('"')[1].split("+")[1]
            )
            if fields["metadata"]["increment_amount"] == 0:
                fields["metadata"]["increment"] = False
            else:
                fields["metadata"]["increment"] = True
        elif "Bullet" in line:
            fields["metadata"]["blitz"] = False
            fields["metadata"]["bullet"] = True
        elif "Rapid" in line:
            fields["metadata"]["blitz"] = False
            fields["metadata"]["rapid"] = True
        elif "Classical" in line:
            fields["metadata"]["blitz"] = False
            fields["metadata"]["classical"] = True
        # no in-progress games
        elif "*" in line:
            counter += 1
            _str = "\nHere, and game_num = " + str(counter)
            fil.write(_str)
            fil.write(line)
            continue
        elif "Result" in line and "0-1" in line:
            fields["metadata"]["white_won"] = False
        elif "Result" in line and "1/2-1/2" in line:
            fields["metadata"]["white_won"] = False
            fields["metadata"]["draw"] = True
        elif "WhiteElo" in line:
            fields["metadata"]["white_elo"] = int(line.split('"')[1])
        elif "BlackElo" in line:
            fields["metadata"]["black_elo"] = int(line.split('"')[1])
        elif "WhiteRatingDiff" in line:
            fields["metadata"]["white_rating_diff"] = int(line.split('"')[1])
        elif "BlackRatingDiff" in line:
            fields["metadata"]["black_rating_diff"] = int(line.split('"')[1])
        elif "ECO" in line:
            fields["metadata"]["eco"] = line.split('"')[1]
        elif "Opening" in line:
            fields["metadata"]["opening"] = line.split('"')[1]
        elif "Termination" in line:
            fields["metadata"]["termination"] = line.split('"')[1]
        elif line[:3] == "1. ":
            fields["game_data"]["long_data"]["game_str"]["game"] = line
            fields["game_data"]["long_data"]["game_str"]["game_id"] = counter
            fields["metadata"]["game_id"] = counter
            fields["game_data"]["game_id"] = counter
            fields["game_data"]["long_data"]["white"]["game_id"] = counter
            fields["game_data"]["long_data"]["black"]["game_id"] = counter
            error = game_parser(line, fields["game_data"], fil)
            if error:
                ctr_str = "\nHere, and game_num = " + str(counter)
                fil.write(ctr_str)
                fil.write(line)
                line_num_str = "\nline_num = " + str(line_num)
                fil.write(line_num_str)
                counter += 1
                continue
            # we ignore this line and then we keep parsing through the lines like nothing happened.
            counter += 1
            yield fields


def main():
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    debug = False
    debug_idx = [8030]
    stop_idx = 30000
    stop_on = True
    data_dict = {"meta_list": [], "game_list": [], "white_list": [], "black_list": []}

    LOG_FILE = os.getcwd() + "\logs.txt"
    if os.path.exists(LOG_FILE):
        os.remove(LOG_FILE)
    if not os.path.exists(LOG_FILE):
        fil = open(LOG_FILE, "a")

    if not debug:
        tables()

    start_time = time.time()
    for full_parsed_game in parse_game_metadata(debug, debug_idx, fil):
        if full_parsed_game["metadata"]["game_id"] == stop_idx:
            break
        if debug:
            if full_parsed_game["metadata"]["game_id"] % 200 == 0:
                print(full_parsed_game["metadata"]["game_id"])
                fil.write(str(full_parsed_game["metadata"]["game_id"]) + "\n")
        else:
            if full_parsed_game["metadata"]["game_id"] % 200 == 0:
                fil.write(str(full_parsed_game["metadata"]["game_id"]) + "\n")
            insert_into_db(
                full_parsed_game,
                full_parsed_game["metadata"]["game_id"] - 1,
                data_dict,
            )
    conn.commit()
    # conn.close()
    fil.close()
    end_time = time.time()

    print("Elapsed time is", end_time - start_time)


if __name__ == "__main__":
    main()
