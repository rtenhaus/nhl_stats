# Sample Script to grab data from NHL API using nhlstats python wrapper

from nhlstats import list_games, list_plays, list_shots, list_shifts
import jsonlines


# Get games for a specified date range
games = list_games('2020-01-01','2020-01-05')

all_games = []  # Empty list to hold modified game data


for game in games:
    # Extract Game ID and Date and create new object to enable efficient loading in BQ
    game_id = game['game_id']
    game_date = game['date']

    new_game = {
        'game_id': game_id,
        'game_date' : game_date,
        'data' : game
    }    

    # Extract Plays from a Game
    plays = list_plays(game_id)
    game_plays = []  # Empty list to hold modified play data

    for play in plays:
        # Add Game ID and Date to new play object to enable efficient loading in BQ
        new_play = {
            'game_id' : game_id,
            'game_date' : game_date,
            'data' : play
        }
        
        game_plays.append(new_play)

    with jsonlines.open(f'../data/plays/plays_{game_id}.jsonl', 'w') as writer:
        writer.write(game_plays)

    # Extract Shots from a Game
    shots = list_shots(game_id)
    game_shots = []  # Empty list to hold modified shot data

    for shot in shots:
        # Add Game ID and Date to new shot object to enable efficient loading in BQ
        new_shot = {
            'game_id' : game_id,
            'game_date' : game_date,
            'data' : shot
        }
        
        game_shots.append(new_shot)

    with jsonlines.open(f'../data/shots/shots_{game_id}.jsonl', 'w') as writer:
        writer.write(game_shots)

    # Extract Shifts from a Game
    shifts = list_shifts(game_id)
    game_shifts = []  # Empty list to hold modified shift data

    for shift in shifts:
        # Add Game ID and Date to new shift object to enable efficient loading in BQ
        new_shift = {
            'game_id' : game_id,
            'game_date' : game_date,
            'data' : shift
        }
        
        game_shifts.append(new_shift)

    with jsonlines.open(f'../data/shifts/shift_{game_id}.jsonl', 'w') as writer:
        writer.write(game_shifts)

    # Log the game
    all_games.append(new_game)    

   
with jsonlines.open('../data/games/all_games.jsonl', 'w') as writer:
    writer.write(games)
