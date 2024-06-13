import pandas as pd

pre_make_pivot_table_df = pd.read_excel("../data/pre_make_pivot_table.xlsx", dtype = {"gameId":"str"})

fromvalue_pivot = pd.pivot_table(
    pre_make_pivot_table_df,
    values = "formvalue",
    index = ["gameId"],
    columns = ["side", "role", "elements"]
).reset_index()
fromvalue_pivot.columns = ['_'.join(col) if isinstance(col, tuple) else col for col in fromvalue_pivot.columns]
fromvalue_pivot.rename(columns = {"gameId__" : "gameId"}, inplace = True)
teamwinrate_df = pre_make_pivot_table_df[["gameId", "side", "teamWinrate"]].drop_duplicates()

teamwinrate_df = teamwinrate_df.pivot(index='gameId', columns='side', values='teamWinrate')

# Subtract Red from Blue to get the new column
teamwinrate_df['teamWinrateDiff'] = teamwinrate_df['Blue'] - teamwinrate_df['Red']
teamwinrate_df.drop(["Blue", "Red"], axis=1, inplace=True)

# Reset index to turn gameId back into a column
teamwinrate_diff_df = teamwinrate_df.reset_index()
headtohead_and_winner_df = pre_make_pivot_table_df[["gameId", "headtoHeadWinrate", "winner"]].drop_duplicates()
final_pivot_table = fromvalue_pivot.merge(teamwinrate_diff_df, on = "gameId", how = "left")
final_pivot_table = final_pivot_table.merge(headtohead_and_winner_df, on = "gameId", how = "left")
final_pivot_table.to_excel("../data/dataset.xlsx")