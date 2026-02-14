-- ================================================
-- ANALYTICAL MARTS (Materialized Views)
-- ================================================
SET search_path TO ipl_analytics;
CREATE MATERIALIZED VIEW mart_death_over_specialists AS WITH death_over_batting AS (
    SELECT p.player_id,
        p.player_name,
        d.season,
        COUNT(*) as death_balls_faced,
        SUM(f.runs_scored) as death_runs_scored,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as death_boundaries,
        SUM(
            CASE
                WHEN f.is_six THEN 1
                ELSE 0
            END
        ) as death_sixes,
        SUM(
            CASE
                WHEN f.is_four THEN 1
                ELSE 0
            END
        ) as death_fours,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as death_dots,
        SUM(
            CASE
                WHEN f.is_wicket THEN 1
                ELSE 0
            END
        ) as times_out,
        COUNT(DISTINCT f.match_id) as matches_batted
    FROM fact_ball_delivery f
        JOIN dim_player p ON f.batter_id = p.player_id
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.match_phase = 'Death'
    GROUP BY p.player_id,
        p.player_name,
        d.season
),
death_over_bowling AS (
    SELECT p.player_id,
        p.player_name,
        d.season,
        COUNT(*) as death_balls_bowled,
        SUM(f.runs_bowler) as death_runs_conceded,
        SUM(
            CASE
                WHEN f.is_wicket THEN 1
                ELSE 0
            END
        ) as death_wickets,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as death_dots_bowled,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as death_boundaries_conceded,
        COUNT(DISTINCT f.match_id) as matches_bowled
    FROM fact_ball_delivery f
        JOIN dim_player p ON f.bowler_id = p.player_id
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.match_phase = 'Death'
        AND f.is_valid_ball = TRUE
    GROUP BY p.player_id,
        p.player_name,
        d.season
)
SELECT COALESCE(bat.player_id, bowl.player_id) as player_id,
    COALESCE(bat.player_name, bowl.player_name) as player_name,
    COALESCE(bat.season, bowl.season) as season,
    -- Batting Stats
    COALESCE(bat.death_balls_faced, 0) as death_balls_faced,
    COALESCE(bat.death_runs_scored, 0) as death_runs_scored,
    COALESCE(bat.death_boundaries, 0) as death_boundaries,
    COALESCE(bat.death_sixes, 0) as death_sixes,
    COALESCE(bat.death_fours, 0) as death_fours,
    COALESCE(bat.matches_batted, 0) as matches_batted,
    -- Batting Metrics
    ROUND(
        CASE
            WHEN bat.death_balls_faced > 0 THEN (bat.death_runs_scored * 100.0) / bat.death_balls_faced
            ELSE 0
        END,
        2
    ) as death_strike_rate,
    ROUND(
        CASE
            WHEN bat.death_balls_faced > 0 THEN (bat.death_boundaries * 100.0) / bat.death_balls_faced
            ELSE 0
        END,
        2
    ) as death_boundary_percentage,
    -- Bowling Stats
    COALESCE(bowl.death_balls_bowled, 0) as death_balls_bowled,
    COALESCE(bowl.death_runs_conceded, 0) as death_runs_conceded,
    COALESCE(bowl.death_wickets, 0) as death_wickets,
    COALESCE(bowl.death_dots_bowled, 0) as death_dots_bowled,
    COALESCE(bowl.matches_bowled, 0) as matches_bowled,
    -- Bowling Metrics
    ROUND(
        CASE
            WHEN bowl.death_balls_bowled > 0 THEN (bowl.death_runs_conceded * 6.0) / bowl.death_balls_bowled
            ELSE 0
        END,
        2
    ) as death_economy_rate,
    ROUND(
        CASE
            WHEN bowl.death_wickets > 0 THEN (bowl.death_balls_bowled * 1.0) / bowl.death_wickets
            ELSE NULL
        END,
        2
    ) as death_bowling_strike_rate,
    ROUND(
        CASE
            WHEN bowl.death_balls_bowled > 0 THEN (bowl.death_dots_bowled * 100.0) / bowl.death_balls_bowled
            ELSE 0
        END,
        2
    ) as death_dot_ball_percentage,
    -- Impact Scores
    ROUND(
        CASE
            WHEN bat.death_balls_faced >= 30 THEN (
                (bat.death_runs_scored * 100.0) / bat.death_balls_faced
            ) * (
                1 + (
                    bat.death_boundaries * 1.0 / bat.death_balls_faced
                )
            )
            ELSE NULL
        END,
        2
    ) as batting_impact_score,
    ROUND(
        CASE
            WHEN bowl.death_balls_bowled >= 30 THEN 100 - (
                (
                    (bowl.death_runs_conceded * 6.0) / bowl.death_balls_bowled
                ) * 5
            ) + (bowl.death_wickets * 10)
            ELSE NULL
        END,
        2
    ) as bowling_impact_score
FROM death_over_batting bat
    FULL OUTER JOIN death_over_bowling bowl ON bat.player_id = bowl.player_id
    AND bat.season = bowl.season
WHERE COALESCE(bat.death_balls_faced, 0) >= 20
    OR COALESCE(bowl.death_balls_bowled, 0) >= 20;
CREATE UNIQUE INDEX idx_death_specialists_pk ON mart_death_over_specialists(player_id, season);
CREATE INDEX idx_death_specialists_season ON mart_death_over_specialists(season);
CREATE INDEX idx_death_bat_impact ON mart_death_over_specialists(batting_impact_score DESC NULLS LAST);
CREATE INDEX idx_death_bowl_impact ON mart_death_over_specialists(bowling_impact_score DESC NULLS LAST);
COMMENT ON MATERIALIZED VIEW mart_death_over_specialists IS 'Death over (16-20) performance analysis for batsmen and bowlers';
CREATE MATERIALIZED VIEW mart_powerplay_performers AS WITH powerplay_batting AS (
    SELECT p.player_id,
        p.player_name,
        p.player_role,
        d.season,
        COUNT(*) as pp_balls_faced,
        SUM(f.runs_scored) as pp_runs,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as pp_boundaries,
        SUM(
            CASE
                WHEN f.is_six THEN 1
                ELSE 0
            END
        ) as pp_sixes,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as pp_dots_played,
        COUNT(DISTINCT f.match_id) as matches_batted
    FROM fact_ball_delivery f
        JOIN dim_player p ON f.batter_id = p.player_id
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.match_phase = 'Powerplay'
    GROUP BY p.player_id,
        p.player_name,
        p.player_role,
        d.season
),
powerplay_bowling AS (
    SELECT p.player_id,
        p.player_name,
        d.season,
        COUNT(*) as pp_balls_bowled,
        SUM(f.runs_bowler) as pp_runs_conceded,
        SUM(
            CASE
                WHEN f.is_wicket THEN 1
                ELSE 0
            END
        ) as pp_wickets,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as pp_dots_bowled,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as pp_boundaries_conceded,
        COUNT(DISTINCT f.match_id) as matches_bowled
    FROM fact_ball_delivery f
        JOIN dim_player p ON f.bowler_id = p.player_id
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.match_phase = 'Powerplay'
        AND f.is_valid_ball = TRUE
    GROUP BY p.player_id,
        p.player_name,
        d.season
)
SELECT COALESCE(bat.player_id, bowl.player_id) as player_id,
    COALESCE(bat.player_name, bowl.player_name) as player_name,
    bat.player_role,
    COALESCE(bat.season, bowl.season) as season,
    -- Batting Stats
    COALESCE(bat.pp_balls_faced, 0) as pp_balls_faced,
    COALESCE(bat.pp_runs, 0) as pp_runs,
    COALESCE(bat.pp_boundaries, 0) as pp_boundaries,
    COALESCE(bat.pp_sixes, 0) as pp_sixes,
    COALESCE(bat.matches_batted, 0) as matches_batted,
    -- Batting Metrics
    ROUND(
        CASE
            WHEN bat.pp_balls_faced > 0 THEN (bat.pp_runs * 100.0) / bat.pp_balls_faced
            ELSE 0
        END,
        2
    ) as pp_strike_rate,
    ROUND(
        CASE
            WHEN bat.pp_balls_faced > 0 THEN (bat.pp_boundaries * 100.0) / bat.pp_balls_faced
            ELSE 0
        END,
        2
    ) as pp_boundary_percentage,
    -- Bowling Stats
    COALESCE(bowl.pp_balls_bowled, 0) as pp_balls_bowled,
    COALESCE(bowl.pp_runs_conceded, 0) as pp_runs_conceded,
    COALESCE(bowl.pp_wickets, 0) as pp_wickets,
    COALESCE(bowl.pp_dots_bowled, 0) as pp_dots_bowled,
    COALESCE(bowl.matches_bowled, 0) as matches_bowled,
    -- Bowling Metrics
    ROUND(
        CASE
            WHEN bowl.pp_balls_bowled > 0 THEN (bowl.pp_runs_conceded * 6.0) / bowl.pp_balls_bowled
            ELSE 0
        END,
        2
    ) as pp_economy_rate,
    ROUND(
        CASE
            WHEN bowl.pp_balls_bowled > 0 THEN (bowl.pp_dots_bowled * 100.0) / bowl.pp_balls_bowled
            ELSE 0
        END,
        2
    ) as pp_dot_ball_pct,
    ROUND(
        CASE
            WHEN bowl.pp_wickets > 0 THEN (bowl.pp_balls_bowled * 1.0) / bowl.pp_wickets
            ELSE NULL
        END,
        2
    ) as pp_bowling_strike_rate
FROM powerplay_batting bat
    FULL OUTER JOIN powerplay_bowling bowl ON bat.player_id = bowl.player_id
    AND bat.season = bowl.season
WHERE COALESCE(bat.pp_balls_faced, 0) >= 18
    OR COALESCE(bowl.pp_balls_bowled, 0) >= 18;
CREATE UNIQUE INDEX idx_powerplay_pk ON mart_powerplay_performers(player_id, season);
CREATE INDEX idx_powerplay_season ON mart_powerplay_performers(season);
CREATE INDEX idx_powerplay_sr ON mart_powerplay_performers(pp_strike_rate DESC);
CREATE INDEX idx_powerplay_economy ON mart_powerplay_performers(pp_economy_rate ASC);
COMMENT ON MATERIALIZED VIEW mart_powerplay_performers IS 'Powerplay (overs 1-6) performance analysis';
CREATE MATERIALIZED VIEW mart_pressure_performance AS WITH pressure_balls AS (
    SELECT f.*,
        CASE
            WHEN f.required_run_rate > 12 THEN 'Extreme'
            WHEN f.required_run_rate > 9 THEN 'High'
            WHEN f.required_run_rate > 6 THEN 'Medium'
            ELSE 'Low'
        END as pressure_level
    FROM fact_ball_delivery f
    WHERE f.innings = 2
        AND f.balls_remaining <= 60
        AND f.balls_remaining > 0
        AND f.runs_required > 0
)
SELECT p.player_id,
    p.player_name,
    d.season,
    pb.pressure_level,
    -- Performance Metrics
    COUNT(*) as balls_in_pressure,
    SUM(pb.runs_scored) as runs_in_pressure,
    SUM(
        CASE
            WHEN pb.is_boundary THEN 1
            ELSE 0
        END
    ) as boundaries_in_pressure,
    SUM(
        CASE
            WHEN pb.is_wicket THEN 1
            ELSE 0
        END
    ) as wickets_in_pressure,
    COUNT(DISTINCT pb.match_id) as matches_in_pressure,
    -- Success Metrics
    ROUND(AVG(pb.runs_scored), 3) as avg_runs_per_ball,
    ROUND(
        (SUM(pb.runs_scored) * 100.0) / NULLIF(COUNT(*), 0),
        2
    ) as strike_rate_under_pressure,
    -- Match Outcomes
    SUM(
        CASE
            WHEN ms.match_winner_id = pb.batting_team_id THEN 1
            ELSE 0
        END
    ) as matches_won,
    SUM(
        CASE
            WHEN ms.match_winner_id != pb.batting_team_id THEN 1
            ELSE 0
        END
    ) as matches_lost,
    ROUND(
        (
            SUM(
                CASE
                    WHEN ms.match_winner_id = pb.batting_team_id THEN 1
                    ELSE 0
                END
            ) * 100.0
        ) / NULLIF(COUNT(DISTINCT pb.match_id), 0),
        2
    ) as win_percentage,
    -- Pressure Performance Index
    ROUND(
        (
            (SUM(pb.runs_scored) * 100.0) / NULLIF(COUNT(*), 0)
        ) * (
            1 - (
                SUM(
                    CASE
                        WHEN pb.is_wicket THEN 1
                        ELSE 0
                    END
                ) * 1.0 / NULLIF(COUNT(DISTINCT pb.match_id), 0)
            )
        ) * (
            1 + (
                SUM(
                    CASE
                        WHEN ms.match_winner_id = pb.batting_team_id THEN 1
                        ELSE 0
                    END
                ) * 1.0 / NULLIF(COUNT(DISTINCT pb.match_id), 0)
            )
        ),
        2
    ) as pressure_performance_index
FROM pressure_balls pb
    JOIN dim_player p ON pb.batter_id = p.player_id
    JOIN dim_date d ON pb.date_id = d.date_id
    JOIN fact_match_summary ms ON pb.match_id = ms.match_id
GROUP BY p.player_id,
    p.player_name,
    d.season,
    pb.pressure_level
HAVING COUNT(*) >= 12
ORDER BY season DESC,
    pressure_performance_index DESC;
CREATE INDEX idx_pressure_player_season ON mart_pressure_performance(player_id, season);
CREATE INDEX idx_pressure_level ON mart_pressure_performance(pressure_level);
CREATE INDEX idx_pressure_index ON mart_pressure_performance(pressure_performance_index DESC);
COMMENT ON MATERIALIZED VIEW mart_pressure_performance IS 'Performance analysis in pressure situations (chasing, last 10 overs)';
CREATE MATERIALIZED VIEW mart_partnership_analysis AS WITH partnerships AS (
    SELECT f.match_id,
        f.innings,
        f.batter_id as player1_id,
        f.non_striker_id as player2_id,
        f.batting_team_id,
        d.season,
        MIN(f.ball_sequence) as start_ball,
        MAX(f.ball_sequence) as end_ball,
        SUM(f.runs_scored) as partnership_runs,
        COUNT(*) as balls_faced,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as boundaries,
        SUM(
            CASE
                WHEN f.is_six THEN 1
                ELSE 0
            END
        ) as sixes,
        MAX(
            CASE
                WHEN f.is_wicket THEN f.batter_id
            END
        ) as dismissed_player_id
    FROM fact_ball_delivery f
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.non_striker_id IS NOT NULL
        AND f.batter_id != f.non_striker_id
    GROUP BY f.match_id,
        f.innings,
        f.batter_id,
        f.non_striker_id,
        f.batting_team_id,
        d.season
    HAVING SUM(f.runs_scored) >= 10
)
SELECT CASE
        WHEN p1.player_id < p2.player_id THEN p1.player_id
        ELSE p2.player_id
    END as player1_id,
    CASE
        WHEN p1.player_id < p2.player_id THEN p1.player_name
        ELSE p2.player_name
    END as player1_name,
    CASE
        WHEN p1.player_id < p2.player_id THEN p2.player_id
        ELSE p1.player_id
    END as player2_id,
    CASE
        WHEN p1.player_id < p2.player_id THEN p2.player_name
        ELSE p1.player_name
    END as player2_name,
    t.team_name,
    ptn.season,
    -- Partnership Stats
    COUNT(*) as total_partnerships,
    SUM(ptn.partnership_runs) as total_partnership_runs,
    MAX(ptn.partnership_runs) as highest_partnership,
    ROUND(AVG(ptn.partnership_runs), 2) as avg_partnership_runs,
    ROUND(AVG(ptn.balls_faced), 1) as avg_balls_faced,
    -- Run Rate
    ROUND(
        (SUM(ptn.partnership_runs) * 6.0) / NULLIF(SUM(ptn.balls_faced), 0),
        2
    ) as partnership_run_rate,
    -- Boundary Stats
    SUM(ptn.boundaries) as total_boundaries,
    ROUND(
        (SUM(ptn.boundaries) * 4.0 * 100.0) / NULLIF(SUM(ptn.partnership_runs), 0),
        2
    ) as boundary_contribution_pct,
    -- Milestones
    SUM(
        CASE
            WHEN ptn.partnership_runs >= 50
            AND ptn.partnership_runs < 100 THEN 1
            ELSE 0
        END
    ) as fifty_plus_partnerships,
    SUM(
        CASE
            WHEN ptn.partnership_runs >= 100 THEN 1
            ELSE 0
        END
    ) as century_partnerships,
    -- Success Rate
    ROUND(
        (
            SUM(
                CASE
                    WHEN ptn.partnership_runs >= 30 THEN 1
                    ELSE 0
                END
            ) * 100.0
        ) / NULLIF(COUNT(*), 0),
        2
    ) as productive_partnership_pct
FROM partnerships ptn
    JOIN dim_player p1 ON ptn.player1_id = p1.player_id
    JOIN dim_player p2 ON ptn.player2_id = p2.player_id
    JOIN dim_team t ON ptn.batting_team_id = t.team_id
GROUP BY CASE
        WHEN p1.player_id < p2.player_id THEN p1.player_id
        ELSE p2.player_id
    END,
    CASE
        WHEN p1.player_id < p2.player_id THEN p1.player_name
        ELSE p2.player_name
    END,
    CASE
        WHEN p1.player_id < p2.player_id THEN p2.player_id
        ELSE p1.player_id
    END,
    CASE
        WHEN p1.player_id < p2.player_id THEN p2.player_name
        ELSE p1.player_name
    END,
    t.team_name,
    ptn.season
HAVING COUNT(*) >= 3
ORDER BY total_partnership_runs DESC;
CREATE INDEX idx_partnership_players ON mart_partnership_analysis(player1_id, player2_id);
CREATE INDEX idx_partnership_season ON mart_partnership_analysis(season);
CREATE INDEX idx_partnership_total ON mart_partnership_analysis(total_partnership_runs DESC);
COMMENT ON MATERIALIZED VIEW mart_partnership_analysis IS 'Batting partnership analysis between players';
CREATE MATERIALIZED VIEW mart_venue_analytics AS
SELECT v.venue_id,
    v.venue_name,
    v.city,
    d.season,
    -- Match Count
    COUNT(DISTINCT f.match_id) as total_matches,
    COUNT(
        DISTINCT CASE
            WHEN f.innings = 1 THEN f.match_id
        END
    ) as total_innings,
    -- Scoring Patterns
    ROUND(
        AVG(
            CASE
                WHEN f.innings = 1 THEN f.team_runs
            END
        ),
        2
    ) as avg_first_innings_score,
    ROUND(
        AVG(
            CASE
                WHEN f.innings = 2 THEN f.team_runs
            END
        ),
        2
    ) as avg_second_innings_score,
    MAX(f.team_runs) as highest_team_total,
    MIN(f.team_runs) as lowest_team_total,
    -- Ball-by-Ball Metrics
    ROUND(AVG(f.runs_scored), 3) as avg_runs_per_ball,
    ROUND(
        AVG(
            CASE
                WHEN f.is_boundary THEN 1.0
                ELSE 0.0
            END
        ) * 100,
        2
    ) as boundary_percentage,
    ROUND(
        AVG(
            CASE
                WHEN f.is_wicket THEN 1.0
                ELSE 0.0
            END
        ) * 100,
        2
    ) as wicket_percentage,
    ROUND(
        AVG(
            CASE
                WHEN f.is_dot_ball THEN 1.0
                ELSE 0.0
            END
        ) * 100,
        2
    ) as dot_ball_percentage,
    -- Phase-wise Run Rates
    ROUND(
        AVG(
            CASE
                WHEN f.match_phase = 'Powerplay' THEN f.runs_scored * 6.0
            END
        ),
        2
    ) as powerplay_run_rate,
    ROUND(
        AVG(
            CASE
                WHEN f.match_phase = 'Middle' THEN f.runs_scored * 6.0
            END
        ),
        2
    ) as middle_run_rate,
    ROUND(
        AVG(
            CASE
                WHEN f.match_phase = 'Death' THEN f.runs_scored * 6.0
            END
        ),
        2
    ) as death_run_rate,
    -- Win Patterns
    SUM(
        CASE
            WHEN ms.toss_decision = 'bat'
            AND ms.match_winner_id = ms.toss_winner_id THEN 1
            ELSE 0
        END
    ) as bat_first_wins,
    SUM(
        CASE
            WHEN ms.toss_decision = 'field'
            AND ms.match_winner_id = ms.toss_winner_id THEN 1
            ELSE 0
        END
    ) as chase_wins,
    -- Toss Advantage
    ROUND(
        (
            SUM(
                CASE
                    WHEN ms.match_winner_id = ms.toss_winner_id THEN 1.0
                    ELSE 0.0
                END
            ) * 100.0
        ) / NULLIF(COUNT(DISTINCT f.match_id), 0),
        2
    ) as toss_win_percentage,
    CASE
        WHEN AVG(f.runs_scored) > 0.18 THEN 'Batting Paradise'
        WHEN AVG(
            CASE
                WHEN f.is_wicket THEN 1.0
                ELSE 0.0
            END
        ) > 0.03 THEN 'Bowler Friendly'
        ELSE 'Balanced'
    END as venue_type,
    -- Home Advantage (if applicable)
    ROUND(
        (
            SUM(
                CASE
                    WHEN (
                        t1.home_city = v.city
                        OR t2.home_city = v.city
                    )
                    AND (
                        (
                            ms.team1_id = t1.team_id
                            AND ms.match_winner_id = t1.team_id
                            AND t1.home_city = v.city
                        )
                        OR (
                            ms.team2_id = t2.team_id
                            AND ms.match_winner_id = t2.team_id
                            AND t2.home_city = v.city
                        )
                    ) THEN 1.0
                    ELSE 0.0
                END
            ) * 100.0
        ) / NULLIF(
            SUM(
                CASE
                    WHEN t1.home_city = v.city
                    OR t2.home_city = v.city THEN 1.0
                    ELSE 0.0
                END
            ),
            0
        ),
        2
    ) as home_team_win_pct
FROM fact_ball_delivery f
    JOIN dim_venue v ON f.venue_id = v.venue_id
    JOIN dim_date d ON f.date_id = d.date_id
    JOIN fact_match_summary ms ON f.match_id = ms.match_id
    LEFT JOIN dim_team t1 ON ms.team1_id = t1.team_id
    LEFT JOIN dim_team t2 ON ms.team2_id = t2.team_id
GROUP BY v.venue_id,
    v.venue_name,
    v.city,
    d.season
HAVING COUNT(DISTINCT f.match_id) >= 5
ORDER BY season DESC,
    total_matches DESC;
CREATE INDEX idx_venue_analytics_venue ON mart_venue_analytics(venue_id);
CREATE INDEX idx_venue_analytics_season ON mart_venue_analytics(season);
CREATE INDEX idx_venue_analytics_type ON mart_venue_analytics(venue_type);
COMMENT ON MATERIALIZED VIEW mart_venue_analytics IS 'Comprehensive venue performance and characteristics analysis';
-- ================================================
-- MART 6: Player Comprehensive Stats
-- ================================================
CREATE MATERIALIZED VIEW mart_player_stats AS WITH batting_stats AS (
    SELECT f.batter_id as player_id,
        d.season,
        COUNT(DISTINCT f.match_id) as matches_batted,
        COUNT(DISTINCT CONCAT(f.match_id, '-', f.innings)) as innings_batted,
        SUM(f.runs_scored) as total_runs,
        COUNT(*) as balls_faced,
        SUM(
            CASE
                WHEN f.is_six THEN 1
                ELSE 0
            END
        ) as sixes,
        SUM(
            CASE
                WHEN f.is_four THEN 1
                ELSE 0
            END
        ) as fours,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as boundaries,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as dots_played,
        SUM(
            CASE
                WHEN f.is_wicket
                AND f.batter_id = f.player_out_id THEN 1
                ELSE 0
            END
        ) as times_dismissed,
        MAX(f.batter_runs) as highest_score
    FROM fact_ball_delivery f
        JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY f.batter_id,
        d.season
),
bowling_stats AS (
    SELECT f.bowler_id as player_id,
        d.season,
        COUNT(DISTINCT f.match_id) as matches_bowled,
        COUNT(
            CASE
                WHEN f.is_valid_ball THEN 1
            END
        ) as balls_bowled,
        SUM(f.runs_bowler) as runs_conceded,
        SUM(
            CASE
                WHEN f.is_wicket THEN 1
                ELSE 0
            END
        ) as wickets,
        SUM(
            CASE
                WHEN f.is_dot_ball THEN 1
                ELSE 0
            END
        ) as dot_balls_bowled,
        SUM(
            CASE
                WHEN f.is_boundary THEN 1
                ELSE 0
            END
        ) as boundaries_conceded,
        MAX(
            (
                SELECT COUNT(*)
                FROM fact_ball_delivery f2
                WHERE f2.match_id = f.match_id
                    AND f2.innings = f.innings
                    AND f2.bowler_id = f.bowler_id
                    AND f2.is_wicket = TRUE
            )
        ) as best_bowling_wickets
    FROM fact_ball_delivery f
        JOIN dim_date d ON f.date_id = d.date_id
    WHERE f.is_valid_ball = TRUE
    GROUP BY f.bowler_id,
        d.season
)
SELECT p.player_id,
    p.player_name,
    p.player_role,
    COALESCE(bs.season, bws.season) as season,
    -- Batting Stats
    COALESCE(bs.matches_batted, 0) as matches_batted,
    COALESCE(bs.innings_batted, 0) as innings_batted,
    COALESCE(bs.total_runs, 0) as runs,
    COALESCE(bs.balls_faced, 0) as balls_faced,
    COALESCE(bs.highest_score, 0) as highest_score,
    COALESCE(bs.fours, 0) as fours,
    COALESCE(bs.sixes, 0) as sixes,
    COALESCE(bs.boundaries, 0) as total_boundaries,
    -- Batting Calculated Metrics
    ROUND(
        CASE
            WHEN bs.times_dismissed > 0 THEN bs.total_runs * 1.0 / bs.times_dismissed
            ELSE NULL
        END,
        2
    ) as batting_average,
    ROUND(
        CASE
            WHEN bs.balls_faced > 0 THEN (bs.total_runs * 100.0) / bs.balls_faced
            ELSE 0
        END,
        2
    ) as strike_rate,
    ROUND(
        CASE
            WHEN bs.balls_faced > 0 THEN (bs.dots_played * 100.0) / bs.balls_faced
            ELSE 0
        END,
        2
    ) as dot_ball_percentage,
    ROUND(
        CASE
            WHEN bs.balls_faced > 0 THEN (bs.boundaries * 100.0) / bs.balls_faced
            ELSE 0
        END,
        2
    ) as boundary_percentage,
    -- Milestones
    SUM(
        CASE
            WHEN bs.highest_score >= 50
            AND bs.highest_score < 100 THEN 1
            ELSE 0
        END
    ) OVER (
        PARTITION BY p.player_id,
        COALESCE(bs.season, bws.season)
    ) as fifties,
    SUM(
        CASE
            WHEN bs.highest_score >= 100 THEN 1
            ELSE 0
        END
    ) OVER (
        PARTITION BY p.player_id,
        COALESCE(bs.season, bws.season)
    ) as hundreds,
    -- Bowling Stats
    COALESCE(bws.matches_bowled, 0) as matches_bowled,
    ROUND(COALESCE(bws.balls_bowled, 0) / 6.0, 1) as overs_bowled,
    COALESCE(bws.balls_bowled, 0) as balls_bowled,
    COALESCE(bws.runs_conceded, 0) as runs_conceded,
    COALESCE(bws.wickets, 0) as wickets,
    COALESCE(bws.dot_balls_bowled, 0) as dot_balls_bowled,
    -- Bowling Calculated Metrics
    ROUND(
        CASE
            WHEN bws.balls_bowled > 0 THEN (bws.runs_conceded * 6.0) / bws.balls_bowled
            ELSE 0
        END,
        2
    ) as economy_rate,
    ROUND(
        CASE
            WHEN bws.wickets > 0 THEN bws.runs_conceded * 1.0 / bws.wickets
            ELSE NULL
        END,
        2
    ) as bowling_average,
    ROUND(
        CASE
            WHEN bws.wickets > 0 THEN bws.balls_bowled * 1.0 / bws.wickets
            ELSE NULL
        END,
        2
    ) as bowling_strike_rate,
    ROUND(
        CASE
            WHEN bws.balls_bowled > 0 THEN (bws.dot_balls_bowled * 100.0) / bws.balls_bowled
            ELSE 0
        END,
        2
    ) as bowling_dot_ball_percentage
FROM dim_player p
    LEFT JOIN batting_stats bs ON p.player_id = bs.player_id
    LEFT JOIN bowling_stats bws ON p.player_id = bws.player_id
    AND bs.season = bws.season
WHERE bs.player_id IS NOT NULL
    OR bws.player_id IS NOT NULL
ORDER BY season DESC,
    runs DESC;
CREATE INDEX idx_player_stats_player ON mart_player_stats(player_id);
CREATE INDEX idx_player_stats_season ON mart_player_stats(season);
CREATE INDEX idx_player_stats_runs ON mart_player_stats(runs DESC);
CREATE INDEX idx_player_stats_wickets ON mart_player_stats(wickets DESC);
COMMENT ON MATERIALIZED VIEW mart_player_stats IS 'Comprehensive player statistics - batting and bowling';