#!/usr/bin/env python3
"""Column descriptions for the silver and gold dbt models (KNOWN_ISSUES.md #4).

    uv run python scripts/write_column_docs.py      # from Gamebot/; rewrites dbt/models/{silver,gold}/schema.yml

Keeps each model's existing description and tests, adds a `columns:` block with a
one-line description per column, in the column order of the current gamebot-lite
export (../preferencespace/static/survivor/gamebot/data/schema.json when present,
else the SQLite file). A column with no entry in DOCS below gets a generic line
derived from its name, so the file is complete and the gaps are greppable ("(no
description yet)"). Descriptions are one sentence, no trailing period, and mean
what the model SQL computes.
"""

from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODELS = ROOT / "dbt/models"
SCHEMA_JSON = ROOT.parent / "preferencespace/static/survivor/gamebot/data/schema.json"
DB = ROOT / "gamebot_lite/data/gamebot.sqlite"

# shared keys and grain
COMMON = {
    "castaway_id": "survivoR castaway id, unique across versions (US0011, AU0001)",
    "version_season": "Version and season code (US07, AU01); the season key everywhere in the warehouse",
    "version": "Show version: US, AU, NZ, SA, UK",
    "episode": "Episode number within the season",
    "day": "Day of the game the event falls on",
    "tribe": "Tribe the castaway belonged to at the event",
    "tribe_status": "Original, swapped or merged tribe at the event",
    "merge_phase": "pre_merge or post_merge at the event",
    "created_at": "When the dbt model wrote the row",
    "updated_at": "When the dbt model last rewrote the row",
    "season": "Season number within the version",
    "season_name": "The season's title (Survivor: Pearl Islands)",
    "full_name": "Castaway's full name",
    "castaway": "Castaway's short name as shown on the broadcast",
    "gender": "Gender as recorded by survivoR",
    "race": "Race as recorded by survivoR, when known",
    "ethnicity": "Ethnicity as recorded by survivoR, when known",
    "african": "1 when survivoR records the castaway as African American",
    "asian": "1 when survivoR records the castaway as Asian American",
    "latin_american": "1 when survivoR records the castaway as Latin American",
    "native_american": "1 when survivoR records the castaway as Native American",
    "bipoc": "1 when any of the four BIPOC flags is set",
    "lgbt": "1 when survivoR records the castaway as LGBT",
}

DOCS = {
  "advantage_strategy": {
    "advantage_strategy_key": "Row key, a hash of castaway, season, advantage and sequence",
    "advantage_id": "survivoR advantage id",
    "sequence_id": "Order of this event in the advantage's life (found, transferred, played)",
    "event_type": "survivoR event: found, played, transferred, expired, ...",
    "target_castaway_id": "Castaway the play was aimed at, when the event has a target",
    "co_castaway_ids": "Other castaways involved in the event, comma-separated",
    "joint_play": "1 when more than one castaway played the advantage together",
    "multi_target_play": "1 when the play named more than one target",
    "success_outcome": "survivoR's outcome of the play (worked, wasted, ...)",
    "votes_nullified": "Votes the play cancelled",
    "advantage_type": "Kind of advantage: hidden immunity idol, extra vote, steal a vote, ...",
    "clue_details": "How the clue to the advantage was obtained, when recorded",
    "location_found": "Where the advantage was found",
    "conditions": "Conditions attached to the advantage, when any",
    "event_category": "found, played, received, shared or other: the event type grouped",
    "advantage_category": "idol, immunity, vote_modifier, advantage or other: the advantage type grouped",
    "played_successfully": "1 when the event is a play and survivoR marks its success yes",
    "played_unsuccessfully": "1 when the event is a play marked no or not needed",
    "played_for_self": "1 when a play named no beneficiary or the player themself",
    "played_for_others": "1 when a play named someone else as its beneficiary",
  },
  "castaway_profile": {
    "age": "Age during the season",
    "city": "Home city at the time of the season",
    "state": "Home state or region at the time of the season",
    "personality_type": "Myers-Briggs type as recorded by survivoR, when known",
    "occupation": "Occupation as shown on the broadcast",
    "season_location": "Where the season was filmed",
    "season_country": "Country the season was filmed in",
    "tribe_setup": "Starting tribe structure of the season (two tribes of eight, ...)",
    "viewers_premiere": "US viewers of the premiere, millions",
    "viewers_finale": "US viewers of the finale, millions",
    "viewers_reunion": "US viewers of the reunion, millions",
    "season_avg_viewers": "Mean US viewers across the season, millions",
  },
  "challenge_performance": {
    "challenge_performance_key": "Row key, a hash of castaway, season and challenge",
    "challenge_id": "survivoR challenge id",
    "challenge_type": "Immunity, Reward, or both, as broadcast",
    "challenge_name": "The challenge's name in survivoR",
    "recurring_name": "The recurring challenge family, when the challenge is one",
    "result": "Lowercased survivoR result: won, lost, draw, won (reward only), won (immunity only)",
    "won_flag": "1 when the result starts with won",
    "chosen_for_reward": "1 when the castaway was chosen to share a reward without winning",
    "sit_out": "1 when the castaway sat the challenge out",
    "order_of_finish": "Finishing position when the challenge records one",
    "team": "Team or tribe the castaway competed for",
    "outcome_type": "survivoR outcome_type: individual or team",
    "challenge_format": "individual or team, from outcome_type",
    "balance_win": "1 when won and the challenge tests balance",
    "endurance_win": "1 when won and the challenge tests endurance",
    "knowledge_win": "1 when won and the challenge tests knowledge",
    "memory_win": "1 when won and the challenge tests memory",
    "precision_win": "1 when won and the challenge tests precision",
    "puzzle_win": "1 when won and the challenge has a puzzle",
    "race_win": "1 when won and the challenge is a race",
    "strength_win": "1 when won and the challenge tests strength",
    "water_win": "1 when won and the challenge is in water",
    "balance_participated": "1 when the challenge tests balance",
    "endurance_participated": "1 when the challenge tests endurance",
    "knowledge_participated": "1 when the challenge tests knowledge",
    "memory_participated": "1 when the challenge tests memory",
    "precision_participated": "1 when the challenge tests precision",
    "puzzle_participated": "1 when the challenge has a puzzle",
    "race_participated": "1 when the challenge is a race",
    "strength_participated": "1 when the challenge tests strength",
    "water_participated": "1 when the challenge is in water",
  },
  "edit_features": {
    "edit_features_key": "Row key, a hash of castaway, season and episode",
    "confessional_count": "Confessionals the castaway had in the episode",
    "confessional_time": "Seconds of confessional the castaway had in the episode",
    "expected_confessional_count": "Episode confessionals divided by castaways still in the game",
    "expected_confessional_time": "Episode confessional seconds divided by castaways still in the game",
    "confessional_count_ratio": "confessional_count over expected_confessional_count; 1 is an even share",
    "confessional_time_ratio": "confessional_time over expected_confessional_time; 1 is an even share",
    "has_confessional": "1 when the castaway had at least one confessional",
    "high_confessional_count": "1 when the castaway had three or more confessionals in the episode",
    "high_confessional_time": "1 when the castaway had 60 seconds or more of confessional in the episode",
    "over_edited_count": "1 when confessional_count is above the even-share expectation",
    "over_edited_time": "1 when confessional_time is above the even-share expectation",
    "under_edited_count": "1 when confessional_count is below the even-share expectation",
    "under_edited_time": "1 when confessional_time is below the even-share expectation",
  },
  "jury_analysis": {
    "jury_analysis_key": "Row key, a hash of juror, season and finalist",
    "finalist_id": "Castaway id of the finalist the juror voted for",
    "voted_for_finalist_name": "Name of the finalist the juror voted for",
    "actual_winner_id": "Castaway id of the season's winner",
    "voted_for_winner": "1 when the juror's vote went to the winner",
    "voted_against_winner": "1 when the juror's vote went to another finalist",
    "juror_original_tribe": "The juror's starting tribe",
    "finalist_original_tribe": "The chosen finalist's starting tribe",
    "same_original_tribe": "1 when juror and chosen finalist started on the same tribe",
    "different_original_tribe": "1 when they started on different tribes",
  },
  "season_context": {
    "season_context_key": "Row key, one per version_season",
    "season_number": "Season number within the version",
    "location": "Where the season was filmed",
    "country": "Country the season was filmed in",
    "tribe_setup": "Starting tribe structure of the season",
    "cast_size": "Castaways at the start of the season",
    "tribe_count": "Starting tribes",
    "finalist_count": "Castaways at the final tribal council",
    "jury_count": "Jurors at the final tribal council",
    "premiered": "Premiere date",
    "ended": "Finale date",
    "filming_started": "First day of filming",
    "filming_ended": "Last day of filming",
    "winner_castaway_id": "Castaway id of the winner",
    "winner": "The winner's name",
    "runner_ups": "Runner-up names, comma-separated",
    "final_vote": "The final tribal council vote as broadcast (5-2-0)",
    "timeslot": "Broadcast timeslot",
    "viewers_reunion": "US viewers of the reunion, millions",
    "viewers_premiere": "US viewers of the premiere, millions",
    "viewers_finale": "US viewers of the finale, millions",
    "viewers_mean": "Mean US viewers across the season, millions",
    "rank": "survivoR's rank of the season by viewers",
    "season_era": "early (seasons 1-10), middle (11-20), modern (21-30), new_school (31-40) or new_era (41 on), by season number within the version",
    "is_new_era_format": "1 when the starting cast is 18 or fewer, the new-era format",
    "season_recency_weight": "0 for the version's first season to 1 for its latest, linear in season number",
    "has_edge_of_extinction": "1 when the tribe setup text mentions Edge of Extinction",
    "has_redemption_island": "1 when the tribe setup text mentions Redemption Island",
    "has_tribe_swap": "1 when the tribe setup text mentions a swap",
    "has_merge_twist": "1 when the tribe setup text mentions the merge",
    "total_cast": "Castaways counted from the castaways table",
    "male_count": "Men in the starting cast",
    "female_count": "Women in the starting cast",
    "bipoc_count": "BIPOC castaways in the starting cast",
    "lgbt_count": "LGBT castaways in the starting cast",
    "returnee_count": "Castaways who had played before",
    "male_ratio": "male_count over total_cast",
    "female_ratio": "female_count over total_cast",
    "bipoc_ratio": "bipoc_count over total_cast",
    "lgbt_ratio": "lgbt_count over total_cast",
    "returnee_ratio": "returnee_count over total_cast",
    "average_age": "Mean age of the starting cast",
    "min_age": "Youngest castaway's age",
    "max_age": "Oldest castaway's age",
    "age_stddev": "Standard deviation of the cast's ages",
    "high_diversity_cast": "1 when bipoc_ratio is 0.4 or more",
    "mixed_returnee_cast": "1 when the cast mixes returnees and new players",
    "all_returnee_cast": "1 when every castaway had played before",
    "all_newbie_cast": "1 when no castaway had played before",
  },
  "social_positioning": {
    "social_positioning_key": "Row key, a hash of castaway, season and episode",
    "tribe_size": "Castaways on the tribe in the episode",
    "male_count": "Men on the tribe",
    "female_count": "Women on the tribe",
    "african_count": "African American castaways on the tribe",
    "asian_count": "Asian American castaways on the tribe",
    "latin_american_count": "Latin American castaways on the tribe",
    "native_american_count": "Native American castaways on the tribe",
    "bipoc_count": "BIPOC castaways on the tribe",
    "lgbt_count": "LGBT castaways on the tribe",
    "male_ratio": "male_count over tribe_size",
    "female_ratio": "female_count over tribe_size",
    "african_ratio": "african_count over tribe_size",
    "asian_ratio": "asian_count over tribe_size",
    "latin_american_ratio": "latin_american_count over tribe_size",
    "native_american_ratio": "native_american_count over tribe_size",
    "bipoc_ratio": "bipoc_count over tribe_size",
    "lgbt_ratio": "lgbt_count over tribe_size",
    "same_gender_ratio": "Share of the tribe with the castaway's gender",
    "lgbt_similarity_ratio": "Share of the tribe with the castaway's LGBT status",
    "bipoc_similarity_ratio": "Share of the tribe with the castaway's BIPOC status",
    "original_tribe": "The castaway's starting tribe",
    "original_tribe_members": "Members of the castaway's starting tribe on the current tribe",
    "total_tribe_members": "Castaways on the current tribe",
    "original_tribe_proportion": "After the merge, original_tribe_members over total_tribe_members; null before",
    "original_tribe_status": "After the merge, minority when the starting tribe is under half of the current tribe, else majority; null before the merge",
    "gender_status": "gender_minority when the castaway's gender is under half the tribe, else gender_majority",
    "racial_status": "racial_minority when the castaway's BIPOC status is the smaller share of the tribe, else racial_majority",
    "lgbt_status": "lgbt_minority when the castaway's LGBT status is the smaller share of the tribe, else lgbt_majority",
  },
  "vote_dynamics": {
    "vote_dynamics_key": "Row key, a hash of castaway, season, episode and vote order",
    "target_id": "Castaway id the vote was cast for",
    "voted_out_id": "Castaway id who left at that council",
    "immunity": "survivoR immunity note for the voter at the council",
    "vote_event": "survivoR's event on the vote (idol play, extra vote, ...)",
    "vote_event_outcome": "survivoR's outcome of that event",
    "split_vote": "survivoR split-vote marker",
    "nullified": "1 when the vote was nullified by an idol or advantage",
    "tie": "1 when the council tied",
    "vote_order": "Order of the vote in the council when there was a revote",
    "vote_correct": "1 when the vote went to the castaway who left",
    "vote_incorrect": "1 when it did not",
    "vote_nullified": "1 when the vote did not count",
    "total_voters": "Castaways voting at the council",
    "aligned_votes": "Votes at the council cast for the same target",
    "alignment_ratio": "aligned_votes over total_voters",
    "in_majority_alliance": "1 when alignment_ratio is 0.5 or more",
    "voting_alone": "1 when alignment_ratio is below 0.3",
    "split_vote_scenario": "1 when the council had a planned split vote",
    "tie_vote_scenario": "1 when the council tied",
    "has_immunity": "1 when the voter held immunity at the council",
  },
}

GOLD = {
    "ml_features_key": "Row key, a hash of castaway_id and version_season; one row each",
    "target_winner": "1 when the castaway won the season (outcome, not a feature)",
    "target_finalist": "1 when the castaway reached the final tribal council (outcome)",
    "target_jury": "1 when the castaway sat on the jury (outcome)",
    "target_placement": "Finishing place, 1 for the winner (outcome)",
    "occupation": "Occupation as shown on the broadcast",
    "personality_type": "Myers-Briggs type as recorded by survivoR, when known",
    "current_age": "Age during the season",
    "is_african": COMMON["african"], "is_asian": COMMON["asian"], "is_latin_american": COMMON["latin_american"],
    "is_native_american": COMMON["native_american"], "is_bipoc": COMMON["bipoc"], "is_lgbt": COMMON["lgbt"],
    "season_number": "Season number within the version",
    "season_era": DOCS["season_context"]["season_era"],
    "is_new_era_format": DOCS["season_context"]["is_new_era_format"],
    "season_recency_weight": DOCS["season_context"]["season_recency_weight"],
    "cast_size": "Castaways at the start of the season",
    "finalist_count": "Castaways at the final tribal council",
    "jury_count": "Jurors at the final tribal council",
    "season_male_ratio": "Share of the starting cast who are men",
    "season_female_ratio": "Share of the starting cast who are women",
    "season_bipoc_ratio": "Share of the starting cast who are BIPOC",
    "season_returnee_ratio": "Share of the starting cast who had played before",
    "season_average_age": "Mean age of the starting cast",
    "has_edge_of_extinction": DOCS["season_context"]["has_edge_of_extinction"],
    "has_redemption_island": DOCS["season_context"]["has_redemption_island"],
    "has_tribe_swap": DOCS["season_context"]["has_tribe_swap"],
    "all_newbie_cast": DOCS["season_context"]["all_newbie_cast"],
    "all_returnee_cast": DOCS["season_context"]["all_returnee_cast"],
    "mixed_returnee_cast": DOCS["season_context"]["mixed_returnee_cast"],
    "total_challenges": "Challenges the castaway took part in",
    "challenges_won": "Challenges won, individual and team",
    "individual_wins": "Individual challenges won",
    "team_wins": "Team challenges won",
    "pre_merge_wins": "Challenges won before the merge",
    "post_merge_wins": "Challenges won after the merge",
    "reward_selections": "Times chosen to share a reward without winning it",
    "challenge_sitouts": "Challenges sat out",
    "individual_win_rate": "individual_wins over individual challenges entered",
    "team_win_rate": "team_wins over team challenges entered",
    "balance_wins": "Wins in challenges that test balance",
    "endurance_wins": "Wins in challenges that test endurance",
    "knowledge_wins": "Wins in challenges that test knowledge",
    "memory_wins": "Wins in challenges that test memory",
    "precision_wins": "Wins in challenges that test precision",
    "puzzle_wins": "Wins in challenges with a puzzle",
    "race_wins": "Wins in challenges that are races",
    "strength_wins": "Wins in challenges that test strength",
    "water_wins": "Wins in challenges held in water",
    "balance_participation_rate": "Share of the castaway's challenges that test balance",
    "endurance_participation_rate": "Share that test endurance",
    "puzzle_participation_rate": "Share with a puzzle",
    "strength_participation_rate": "Share that test strength",
    "advantages_found": "Advantages of any kind found",
    "advantages_played": "Advantages of any kind played",
    "idols_found": "Hidden immunity idols found",
    "idols_played": "Hidden immunity idols played",
    "advantages_played_successfully": "Plays that worked",
    "advantages_played_unsuccessfully": "Plays that were wasted or void",
    "advantages_played_for_self": "Plays that targeted the player",
    "advantages_played_for_others": "Plays that targeted someone else",
    "advantage_success_rate": "advantages_played_successfully over advantages_played",
    "idol_success_rate": "Idol plays that worked over idols played",
    "total_votes_cast": "Votes cast at tribal councils",
    "votes_correct": "Votes cast for the castaway who left",
    "votes_incorrect": "Votes cast for someone who stayed",
    "pre_merge_tribals_attended": "Councils attended before the merge",
    "post_merge_tribals_attended": "Councils attended after the merge",
    "pre_merge_correct_votes": "Correct votes before the merge",
    "post_merge_correct_votes": "Correct votes after the merge",
    "majority_alliance_votes": "Votes with alignment_ratio of 0.5 or more",
    "lone_wolf_votes": "Votes with alignment_ratio below 0.3",
    "avg_vote_alignment": "Mean alignment_ratio over the castaway's votes",
    "vote_accuracy_rate": "votes_correct over total_votes_cast",
    "pre_merge_accuracy_rate": "Correct votes over votes cast before the merge",
    "post_merge_accuracy_rate": "Correct votes over votes cast after the merge",
    "majority_alliance_rate": "majority_alliance_votes over total_votes_cast",
    "lone_wolf_rate": "lone_wolf_votes over total_votes_cast",
    "total_votes_received": "Votes cast against the castaway across the season",
    "pre_merge_votes_received": "Votes received before the merge",
    "post_merge_votes_received": "Votes received after the merge",
    "avg_same_gender_ratio": "Mean share of the tribe with the castaway's gender, over episodes",
    "avg_lgbt_similarity_ratio": "Mean share of the tribe with the castaway's LGBT status",
    "avg_bipoc_similarity_ratio": "Mean share of the tribe with the castaway's BIPOC status",
    "avg_original_tribe_strength": "Mean share of the current tribe from the castaway's starting tribe",
    "gender_minority_rate": "Share of episodes the castaway's gender was the tribe minority",
    "racial_minority_rate": "Share of episodes the castaway's BIPOC status was the tribe minority",
    "lgbt_minority_rate": "Share of episodes the castaway's LGBT status was the tribe minority",
    "original_tribe_minority_rate": "Share of episodes the starting tribe was the minority on the current tribe",
    "jury_votes_received": "Jury votes received at the final tribal council",
    "jury_votes_from_original_tribe": "Jury votes from jurors who started on the same tribe",
    "original_tribe_jury_support_rate": "jury_votes_from_original_tribe over jury_votes_received",
    "total_confessional_count": "Confessionals across the season",
    "total_confessional_time": "Seconds of confessional across the season",
    "total_expected_confessional_count": "Sum of the even-share expectation over episodes appeared",
    "total_expected_confessional_time": "Sum of the even-share time expectation over episodes appeared",
    "episodes_with_confessionals": "Episodes with at least one confessional",
    "high_confessional_episodes": "Episodes with three or more confessionals",
    "high_confessional_time_episodes": "Episodes with 60 seconds or more of confessional",
    "over_edited_count_episodes": "Episodes with more confessionals than the even-share expectation",
    "over_edited_time_episodes": "Episodes with more confessional time than the even-share expectation",
    "under_edited_count_episodes": "Episodes with fewer confessionals than the even-share expectation",
    "under_edited_time_episodes": "Episodes with less confessional time than the even-share expectation",
    "total_episodes_appeared": "Episodes the castaway was in the game",
    "overall_confessional_count_ratio": "total_confessional_count over total_expected_confessional_count",
    "overall_confessional_time_ratio": "total_confessional_time over total_expected_confessional_time",
    "confessional_presence_rate": "episodes_with_confessionals over total_episodes_appeared",
    "over_edited_rate": "over_edited_count_episodes over total_episodes_appeared",
    "under_edited_rate": "under_edited_count_episodes over total_episodes_appeared",
    "avg_confessional_count_per_episode": "total_confessional_count over total_episodes_appeared",
    "avg_confessional_time_per_episode": "total_confessional_time over total_episodes_appeared",
    "has_any_confessionals": "1 when the castaway had a confessional in any episode",
    "high_edit_presence": "1 when confessional_presence_rate is 0.5 or more",
    "significantly_over_edited": "1 when the overall count ratio is above 1.2",
    "significantly_under_edited": "1 when the overall count ratio is below 0.8",
    "edit_and_challenge_success": "1 when the castaway had any confessional and won any challenge",
    "edit_and_advantage_success": "1 when the castaway had any confessional and found any advantage",
    "edit_and_strategic_success": "1 when confessional_presence_rate is 0.5 or more and vote_accuracy_rate is 0.7 or more",
}


def columns_of() -> dict[str, list[str]]:
    if SCHEMA_JSON.exists():
        s = json.loads(SCHEMA_JSON.read_text())
        return {t["name"]: [c["name"] for c in t["columns"]] for t in s["tables"] if t["layer"] in ("silver", "gold")}
    con = sqlite3.connect(DB)
    out = {}
    for layer in ("silver", "gold"):
        for f in (MODELS / layer).glob("*.sql"):
            out[f.stem] = [r[1] for r in con.execute(f'pragma table_info("{f.stem}")')]
    return out


def describe(table: str, col: str) -> str:
    d = GOLD if table.startswith("ml_features") else DOCS.get(table, {})
    text = d.get(col) or COMMON.get(col)
    if text is None:
        text = col.replace("_", " ") + " (no description yet)"
    return text


def rewrite(layer: str, cols: dict[str, list[str]]) -> int:
    path = MODELS / layer / "schema.yml"
    text = path.read_text()
    # split into the header and one block per model, keeping each block's description and tests
    parts = re.split(r"(?m)^(?=  - name: )", text)
    head, blocks = parts[0], parts[1:]
    out = [head.rstrip("\n") + "\n"]
    n = 0
    for b in blocks:
        name = re.match(r"  - name:\s*(\S+)", b).group(1)
        # drop any existing columns block, keep the rest
        b = re.sub(r"(?ms)^    columns:\n(?:      .*\n?|\n)*", "", b).rstrip("\n") + "\n"
        lines = ["    columns:"]
        for c in cols.get(name, []):
            desc = describe(name, c).replace('"', "'")
            lines.append(f"      - name: {c}\n        description: \"{desc}\"")
            n += 1
        out.append(b + "\n".join(lines) + "\n\n")
    path.write_text("".join(out).rstrip("\n") + "\n")
    return n


def main():
    cols = columns_of()
    total = 0
    for layer in ("silver", "gold"):
        total += rewrite(layer, cols)
    missing = [(t, c) for t, cs in cols.items() for c in cs if "(no description yet)" in describe(t, c)]
    print(f"wrote {total} column descriptions into dbt/models/silver/schema.yml and gold/schema.yml; {len(missing)} without a hand-written line: {missing[:10]}")


if __name__ == "__main__":
    main()
