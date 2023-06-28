import io
import zstandard as zstd
from pathlib import Path
import json
import psycopg2
from psycopg2 import sql

DCTX = zstd.ZstdDecompressor(max_window_size=2**31)

def insert_into_db(data):
    # Connect to your postgres DB
    conn = psycopg2.connect("dbname=test user=JoeH password=a")
    # Open a cursor to perform database operations
    cur = conn.cursor()

    # execute the INSERT statement
    cur.execute(sql.SQL("INSERT INTO chess_table (column1) VALUES (%s, %s)"), (data['column1'], data['column2']))

    # commit the changes to the database
    conn.commit()

    # close the communication with the database
    cur.close()
    conn.close()

def read_lines_from_zst_file(zstd_file_path:Path):
    with (zstd.open(zstd_file_path, mode='rb', dctx=DCTX) as zfh, io.TextIOWrapper(zfh) as iofh):
        for line in iofh:
            yield line    

def game_parser(game_data):
    length = -1
    game_number_of_blunders = 0
    number_of_white_blunders = 0
    number_of_black_blunders = 0
    game_number_of_mistakes = 0
    number_of_white_mistakes = 0
    number_of_black_mistakes = 0
    game_number_of_inaccuracies = 0
    number_of_white_inaccuracies = 0
    number_of_black_inaccuracies = 0
    most_time_taken = -1
    least_time_taken = -1
    timestamps_white = []
    timestamps_black = []
    has_evals = False
    has_annotations = False
    evals = []
    white_move_list = []
    black_move_list = []
    game_checks = 0
    game_black_checks = 0
    game_white_checks = 0
    num_white_checks = 0
    game_captures = 0
    game_white_captures = 0
    game_black_captures = 0
    game_promotions = 0
    game_black_promotions = 0
    game_black_q_promotions = 0
    game_black_n_promotions = 0
    game_black_b_promotions = 0
    game_black_r_promotions = 0
    game_white_promotions = 0
    game_check_and_promotion = 0
    potentially_ambigous_moves = 0
    if ('\%eval' in game_data[:9]):
        has_evals = True
    if ('[annotations]' in game_data[:15]): #TODO
        has_annotations = True

    split_game_data = game_data.split("}")
    first_move = True
    for move_data in split_game_data:
        if (first_move):
            move_data = move_data.lstrip() # gets rid of annoying first case
            first_move = False
        split_move = move_data.partition("{")
        if (split_move[0][2] == "."): # if uses ... notation, then it is black to move
            from_index = split_move[0].rindex(".") # Find the last dot in the move
            move = split_move[0][from_index+1:].trim() # Remove whitespace

            if (move[0] == "N"):
                pass #TODO How do I want my data to look like
            if ("x" in move):
                game_black_captures += 1
                game_captures += 1
            if ("+" in move):
                game_black_checks += 1
                game_checks += 1
            if ("=" in move):
                game_black_promotions += 1
                game_promotions += 1
                if ("=Q" in move):
                    game_black_q_promotions += 1
                    move = move.replace("=Q", "") # creates new string, but I think is worth, so we don't have to deal with edge cases
                if ("=B" in move):
                    game_black_b_promotions += 1
                    move = move.replace("=B", "")
                if ("=N" in move):
                    game_black_n_promotions += 1
                    move = move.replace("=N", "")
                if ("=R" in move):
                    game_black_r_promotions += 1
                    move = move.replace("=R", "")
                if ("+" in move):
                    game_check_and_promotion += 1



            black_move_list.append(move)
        else:
            pass # white to move -- copy and paste, but change all "black" to "white"

        annot = split_move[1].split("]") # is the part we are now focused on
        if (len(annot) == 2): #clock case
            timestamps_black.append(str(annot[0][7:]))
        elif (len(annot) == 3): #clock and eval case
            evals.append(annot[0][8:])
            time = str(annot[1][8:])
            timestamps_black.append(int(time[0]) * 3600 + int(time[2:4]) * 60 + int(time[5:])) # hh:mm:ss > sec converter
        else:
            raise AssertionError("unexpected value of annot!", annot)



    print(split_game_data[0])
    print(split_game_data[1])
    print(split_game_data[2])
    n = len(split_game_data)
    print(split_game_data[n-2])
    print(split_game_data[n-1])

def parse_game_metadata():
    fields = {
        'rated': True,
        'bullet': False,
        'blitz': True,
        'rapid': False,
        'classical': False,
        'white_won': True,
        'draw': False,
        'white_elo': -1,
        'black_elo': -1,
        'white_rating_diff': 100,
        'black_rating_diff': 100,
        'eco': "Z99",
        'opening': "Literally the Grob",
        'time_control': "-100",
        'bonus': "7",
        'termination': 'KO',
        'increment': False,
        'increment_amount': -1
    }
    for line in read_lines_from_zst_file(file):
        if 'Unrated' in line:
            fields['rated'] = False
        if 'Bullet' in line:
            fields['blitz'] = False
            fields['bullet'] = True
        if  'Rapid' in line:
            fields['blitz'] = False
            fields['rapid'] = True
        if 'Classical' in line:
            fields['blitz'] = False
            fields['classical'] = True
        if '0-1' in line:
            fields['white_won'] = False
        if '1/2-1/2' in line:
            fields['white_won'] = False
            fields['draw'] = True
        if 'WhiteElo' in line:
            fields['white_elo'] = line.split("\"")[1]
        if 'BlackElo' in line:
            fields['black_elo'] = line.split("\"")[1]
        if 'WhiteRatingDiff' in line:
            fields['white_rating_diff'] = line.split("\"")[1]
        if 'BlackRatingDiff' in line:
            fields['black_rating_diff'] = line.split("\"")[1]
        if 'ECO' in line:
            fields['eco'] = line.split("\"")[1]
        if 'Opening' in line:
            fields['opening'] = line.split("\"")[1]
        if 'TimeControl' in line:
            fields['time_control'] = line.split("\"")[1].split("+")[0]
            fields['bonus'] = line.split("\"")[1].split("+")[1]
        if 'Termination' in line:
            fields['termination'] = line.split("\"")[1]
        if (line[:3] == "1. "):
            fields['game'] = line
            fields['length'] = -1 # Hi I need help here TODO
            game_parser(line) 
            yield fields
            fields = { # reset fields after game has been yielded
            'rated': True,
            'bullet': False,
            'blitz': True,
            'rapid': False,
            'classical': False,
            'white_won': True,
            'draw': False,
            'white_elo': -1,
            'black_elo': -1,
            'white_rating_diff': 100,
            'black_rating_diff': 100,
            'eco': "Z99",
            'opening': "Literally the Grob",
            'time_control': "-100+7",
            'termination': 'KO',
            'game': ""
        }

        

                
            


if __name__ == "__main__":
    file = Path("C:\\Users\\JoeH\\Downloads\\lichess_db_standard_rated_2023-05.pgn.zst")
    counter = 0
    for fields in parse_game_metadata():
        print(fields)
        print("\n")
        print("\n")
        counter += 1
        if counter >= 10:
            break
