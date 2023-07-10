import io
import zstandard as zstd
from pathlib import Path
import psycopg2
from psycopg2 import sql
import numpy as np
import copy
import polars as pl
import pandas as pd
from colorama import init as colorama_init
from colorama import Fore
from colorama import Style

colorama_init()

file = Path("C:\\Users\\JoeH\\Downloads\\lichess_db_standard_rated_2023-05.pgn.zst")
DCTX = zstd.ZstdDecompressor(max_window_size=2**31)


def insert_into_db(data, fields):
    # Connect to your postgres DB
    conn = psycopg2.connect("dbname=test user=JoeH password=a")
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # execute the INSERT statement

    q1 = sql.SQL("insert into table ({}) values ({})").format(
        sql.SQL(", ").join(map(sql.Identifier, fields)),
        sql.SQL(", ").join(sql.Placeholder() * len(fields)),
    )
    print(q1.as_string(conn))

    # cur.execute(sql.SQL("INSERT INTO chess_table VALUES (%s, %s)"), (data['column1'], data['column2']))

    # commit the changes to the database
    # conn.commit()

    # close the communication with the database
    # cur.close()
    # conn.close()


def read_lines_from_zst_file(zstd_file_path: Path):
    with (
        zstd.open(zstd_file_path, mode="rb", dctx=DCTX) as zfh,
        io.TextIOWrapper(zfh) as iofh,
    ):
        for line in iofh:
            yield line


def def_fields():
    # piece and location
    size = 256
    # game_black_data = pl.DataFrame(
    #     {f"col{i}": pl.repeat("", size) for i in range(1, 3)}
    # )
    # game_black_data.columns[0] = "piece"
    # game_black_data.columns[1] = "location"
    # game_white_data = pl.DataFrame(
    #     {f"col{i}": pl.repeat("", size) for i in range(1, 3)}
    # )
    # game_white_data.columns[0] = "piece"
    # game_white_data.columns[1] = "location"

    # nmcl_fields = ["move_num", "capture", "castle", "checks", "promotions"]
    # size_nmcl_fields = len(nmcl_fields)
    # str_fields = ["piece", "location"]
    # size_str_fields = len(str_fields)

    # move_num, capture, castle, checks
    # game_black_numerical_data = pl.DataFrame(
    #    {str(nmcl_fields[i - 1]): pl.repeat("", size, eager=True) for i in range(1, 5)}
    # )
    #### Polars method above, pandas below

    # game_black_numerical_data = pd.DataFrame(
    #     columns=nmcl_fields, index=list(range(0, 256))
    # )

    # game_white_numerical_data = pd.DataFrame(
    #     columns=nmcl_fields, index=list(range(0, 256))
    # )

    # game_black_data = pd.DataFrame(columns=str_fields, index=list(range(0, 256)))

    # game_white_data = pd.DataFrame(columns=str_fields, index=list(range(0, 256)))

    # game_white_numerical_data = pl.DataFrame(
    #    {str(nmcl_fields[i - 1]): pl.repeat("", size, eager=True) for i in range(1, 5)}
    # )

    fields = {
        "rated": True,
        "bullet": False,
        "blitz": True,
        "rapid": False,
        "classical": False,
        "white_won": True,
        "draw": False,
        "white_elo": -1,
        "black_elo": -1,
        "white_rating_diff": -1,
        "black_rating_diff": -1,
        "eco": "Z99",
        "opening": "Unknown",
        "time_control": -100,
        "bonus": 7,
        "termination": "KO",
        "increment": False,
        "increment_amount": -1,
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
        "most_time_taken": -1,
        "least_time_taken": -1,
        "timestamps_white": [],
        "timestamps_black": [],
        "has_evals": False,
        "has_annotations": False,
        "evals": [],
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
        # "size_str_fields": size_str_fields,
        # "size_nmcl_fields": size_nmcl_fields,
        #         nmcl_fields = ["move_num", "capture", "castle", "checks", "promotions"]
        # size_nmcl_fields = len(nmcl_fields)
        # str_fields = ["piece", "location"]
        # size_str_fields = len(str_fields)
        "game_black_piece_data": [],
        "game_black_location_data": [],
        "game_white_piece_data": [],
        "game_white_location_data": [],
        "game_white_move_num_data": [],
        "game_white_captures_data": [],
        "game_white_castle_data": [],
        "game_white_checks_data": [],
        "game_white_promotions_data": [],
        "game_black_move_num_data": [],
        "game_black_captures_data": [],
        "game_black_castle_data": [],
        "game_black_checks_data": [],
        "game_black_promotions_data": [],
        "game_black_q_castle": False,
        "game_black_k_castle": False,
        "game_white_q_castle": False,
        "game_white_k_castle": False,
        "game": "",
    }
    return fields


def record_when_and_where_events(color, move, f, move_num):
    if "=" in move:
        f["game_" + color + "_promotions"] += 1
        f["game_promotions"] += 1
        f["game_" + color + "_promotions_data"].append(1)
        if "=Q" in move:
            f["game_" + color + "_q_promotions"] += 1
            # creates new string, but I think is worth, so we don't have to deal with edge cases
            move = move.replace("=Q", "")
        if "=B" in move:
            f["game_" + color + "black_b_promotions"] += 1
            move = move.replace("=B", "")
        if "=N" in move:
            f["game_" + color + "_n_promotions"] += 1
            move = move.replace("=N", "")
        if "=R" in move:
            f["game_" + color + "_r_promotions"] += 1
            move = move.replace("=R", "")
    else:
        f["game_" + color + "_promotions_data"].append(0)

    if "+" in move:
        f["game_" + color + "_checks"] += 1
        f["game_checks"] += 1
        move = move.replace("+", "")
        f["game_" + color + "_checks_data"].append(1)
    else:
        f["game_" + color + "_checks_data"].append(0)

    if "x" in move:
        f["game_" + color + "_captures"] += 1
        f["game_captures"] += 1
        f["game_" + color + "_captures_data"].append(1)
        move = move.replace("x", "")
    else:
        f["game_" + color + "_captures_data"].append(0)
    return move


def piece_ambiguator(str, f, color, move_num, dest, move):
    f["game_" + color + "_piece_data"].append(str)
    if len(move) == 4:
        f["potentially_ambigous_moves"] += 1  # TODO DEAL WITH AMBIGOUS CASE?
        move = move[:1] + move[2:]
    f["game_" + color + "_location_data"].append(dest)


def record_when_and_where_pieces(color, move, f, move_num):
    if "O-O-O" in move:
        if color == "black":
            f["game_black_q_castle"] = True
        if color == "white":
            f["game_white_q_castle"] = True
        dest = "O-O-O"
        f["game_" + color + "_piece_data"].append(
            dest
        )  ##TODO Look at if this is correct
        f["game_" + color + "_location_data"].append(dest)
        # rook somewhere and king somewhere (post processing?)
        f["game_" + color + "_castle_data"].append(1)
    elif "O-O" in move:
        if color == "black":
            f["game_black_k_castle"] = True
        if color == "white":
            f["game_white_k_castle"] = True
        dest = "O-O"
        f["game_" + color + "_k_castle"] = True
        f["game_" + color + "_piece_data"].append(
            dest
        )  ##TODO Look at if this is correct
        f["game_" + color + "_location_data"].append(dest)
        # rook f1 and kg1
        f["game_" + color + "_castle_data"].append(1)
    else:
        f["game_" + color + "_castle_data"].append(0)
        dest = move[-2:]

        if move[0] == "N":
            piece_ambiguator("N", f, color, move_num, dest, move)
        elif move[0] == "B":
            piece_ambiguator("B", f, color, move_num, dest, move)
        elif move[0] == "Q":
            piece_ambiguator("Q", f, color, move_num, dest, move)
        elif move[0] == "K":
            piece_ambiguator("K", f, color, move_num, dest, move)
        elif move[0] == "R":
            piece_ambiguator("R", f, color, move_num, dest, move)
        else:
            f["game_" + color + "_piece_data"].append("P")
            if len(move) == 3:
                f["potentially_ambigous_moves"] += 1
            f["game_" + color + "_location_data"].append(dest)


def annotations_and_timings(color, split_move, f):
    if len(split_move) == 3:  # clock case
        li = split_move[2].rindex("k")
        time = split_move[2][li + 2 : -2]
        f["timestamps_" + color].append(
            int(time[0]) * 3600 + int(time[2:4]) * 60 + int(time[5:])
        )
    elif len(split_move) == 4:  # clock and eval case
        # TODO: check if these splits are correct
        print("here and split_move")
        print(split_move)
        li = split_move[2].rindex("k")
        time = split_move[3][li + 2 : -2]

        f["evals"].append(split_move[3][8:])
        time = str(split_move[2][7:-2])
        # hh:mm:ss > sec converter
        f["timestamps_" + color].append(
            int(time[0]) * 3600 + int(time[2:4]) * 60 + int(time[5:])
        )
    else:
        raise AssertionError("unexpected length of split_move!", split_move)


def game_parser(game_data, f):  # f is our fields
    if "eval" in game_data[:20]:
        f["has_evals"] = True
        print("")
        print(game_data[:20])
        print("")
    if "[annotations]" in game_data[:20]:  # TODO what are examples of annotations
        f["has_annotations"] = True

    split_game_data = game_data.split("}")
    move_num = 0
    for move_data in split_game_data:
        move_num += 1
        f["length"] = move_num
        split_move = move_data.partition("{")
        split_move_notation = split_move[0]
        # if uses ... notation, then it is black to move
        if split_move_notation.rfind(".") != -1 and split_move_notation.rfind(
            "."
        ) == split_move_notation.find("."):
            from_index = split_move_notation.rindex(".")
            # Find the last dot in the move
            move = split_move_notation[(from_index + 2) : -1]
            # Shouldn't be any whitespace
            f["game_white_move_num_data"].append(move_num)

            move = record_when_and_where_events("white", move, f, move_num)
            record_when_and_where_pieces("white", move, f, move_num)
            annotations_and_timings("white", split_move, f)
        elif "-" in split_move_notation:
            return f
        else:
            from_index = split_move_notation.rindex(".")
            # Find the last dot in the move
            move = split_move_notation[(from_index + 2) : -1]
            # Shouldn't be any whitespace
            f["game_black_move_num_data"].append(move_num)

            move = record_when_and_where_events("black", move, f, move_num)
            record_when_and_where_pieces("black", move, f, move_num)
            annotations_and_timings("black", split_move, f)

    return f


def parse_game_metadata():
    fields = def_fields()
    for line in read_lines_from_zst_file(file):
        if "Unrated" in line:
            fields["rated"] = False
        if "Bullet" in line:
            fields["blitz"] = False
            fields["bullet"] = True
        if "Rapid" in line:
            fields["blitz"] = False
            fields["rapid"] = True
        if "Classical" in line:
            fields["blitz"] = False
            fields["classical"] = True
        if "0-1" in line:
            fields["white_won"] = False
        if "1/2-1/2" in line:
            fields["white_won"] = False
            fields["draw"] = True
        if "WhiteElo" in line:
            fields["white_elo"] = line.split('"')[1]
        if "BlackElo" in line:
            fields["black_elo"] = line.split('"')[1]
        if "WhiteRatingDiff" in line:
            fields["white_rating_diff"] = line.split('"')[1]
        if "BlackRatingDiff" in line:
            fields["black_rating_diff"] = line.split('"')[1]
        if "ECO" in line:
            fields["eco"] = line.split('"')[1]
        if "Opening" in line:
            fields["opening"] = line.split('"')[1]
        if "TimeControl" in line:
            fields["time_control"] = line.split('"')[1].split("+")[0]
            fields["bonus"] = line.split('"')[1].split("+")[1]
        if "Termination" in line:
            fields["termination"] = line.split('"')[1]
        if line[:3] == "1. ":
            fields["game"] = line
            # updates some fields in an isolated function
            game_parser(fields["game"], fields)
            fields_return = copy.deepcopy(fields)
            fields = def_fields()
            yield fields_return


def main():
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    print("=================================================================")
    counter = 0
    for full_parsed_game in parse_game_metadata():
        print(counter)
        counter += 1
        if counter == 11:
            print(full_parsed_game)
            break


if __name__ == "__main__":
    main()
