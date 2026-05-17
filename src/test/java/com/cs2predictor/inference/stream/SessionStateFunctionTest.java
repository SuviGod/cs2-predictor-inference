package com.cs2predictor.inference.stream;

import org.junit.jupiter.api.Test;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.List;

import static com.cs2predictor.inference.stream.SessionStateFunction.*;
import static org.junit.jupiter.api.Assertions.*;

class SessionStateFunctionTest {

    // Tests call the package-private static helpers directly (archiveRound,
    // saveRoundStart, computePrev3) so no SparkSession or GroupState mock is needed.

    // Build a minimal allplayers JSON with one CT and one T player.
    // Parameters: steamId, health, cumulative match_stats.kills, state.round_totaldmg
    private static String ap(String ctId, int ctHp, int ctCumKills, int ctDmg,
                              String tId,  int tHp,  int tCumKills,  int tDmg) {
        return String.format(
                "{\"%s\":{\"team\":\"CT\",\"state\":{\"health\":%d,\"round_kills\":0,\"round_totaldmg\":%d},\"match_stats\":{\"kills\":%d}}," +
                " \"%s\":{\"team\":\"T\", \"state\":{\"health\":%d,\"round_kills\":0,\"round_totaldmg\":%d},\"match_stats\":{\"kills\":%d}}}",
                ctId, ctHp, ctDmg, ctCumKills,
                tId,  tHp,  tDmg,  tCumKills);
    }

    @Test
    void first3Rounds_prev3SumsGrowIncrementally() throws Exception {
        MatchSessionState state = new MatchSessionState();
        state.currentRound = 1;

        // --- Round 1 ---
        // Freezetime: start kills are ct1=0, t1=0
        saveRoundStart(state, ap("ct1", 100, 0, 0, "t1", 100, 0, 0));
        state.currentRound = 1;

        // Live tick before any history: prev3 all zero
        int[] p = computePrev3(state, ap("ct1", 100, 2, 80, "t1", 100, 1, 50));
        assertArrayEquals(new int[]{0, 0, 0, 0}, p, "No history yet");

        // Over: ct1 killed 2 (cum 2 − start 0), dmg 80; t1 killed 1, dmg 50
        archiveRound(state, ap("ct1", 100, 2, 80, "t1", 100, 1, 50));
        state.roundArchived = true;

        // --- Round 2 ---
        saveRoundStart(state, ap("ct1", 100, 2, 0, "t1", 100, 1, 0));
        state.currentRound = 2;
        state.roundArchived = false;

        p = computePrev3(state, ap("ct1", 100, 3, 60, "t1", 100, 2, 40));
        assertArrayEquals(new int[]{2, 80, 1, 50}, p, "Round 2 live: only round-1 history");

        // Over: ct1 killed 1 (cum 3 − start 2), dmg 60; t1 killed 1 (2−1), dmg 40
        archiveRound(state, ap("ct1", 100, 3, 60, "t1", 100, 2, 40));
        state.roundArchived = true;

        // --- Round 3 ---
        saveRoundStart(state, ap("ct1", 100, 3, 0, "t1", 100, 2, 0));
        state.currentRound = 3;
        state.roundArchived = false;

        p = computePrev3(state, ap("ct1", 100, 4, 70, "t1", 100, 3, 30));
        // ct: 2+1=3 kills, 80+60=140 dmg;  t: 1+1=2 kills, 50+40=90 dmg
        assertArrayEquals(new int[]{3, 140, 2, 90}, p, "Round 3 live: rounds 1+2 summed");
    }

    @Test
    void round4Plus_slidingWindowOfExactly3Rounds() throws Exception {
        MatchSessionState state = new MatchSessionState();
        // Pre-load 3 rounds of history (rounds 1-3)
        state.playerHistory.put("ct1", new ArrayDeque<>(List.of(
                new RoundStats(1, 10),
                new RoundStats(2, 20),
                new RoundStats(3, 30)
        )));
        state.killsAtRoundStart.put("ct1", 6);   // cumulative kills at start of round 4
        state.currentRound = 4;

        // Live: sum of rounds 1-3 = 6 kills, 60 dmg
        int[] p = computePrev3(state, ap("ct1", 100, 8, 40, "t1", 0, 0, 0));
        assertEquals(6, p[0], "ct kills prev3");
        assertEquals(60, p[1], "ct dmg prev3");

        // Over round 4: ct1 killed 2 (cum 8 − start 6), dmg 40
        archiveRound(state, ap("ct1", 100, 8, 40, "t1", 0, 0, 0));

        Deque<RoundStats> hist = state.playerHistory.get("ct1");
        assertEquals(3, hist.size(), "History stays capped at 3 after eviction");
        // Round 1 entry (kills=1) must have been evicted; oldest is now round 2 (kills=2)
        assertEquals(2, hist.peekFirst().kills(), "Round 1 evicted; oldest is round 2");
        assertEquals(2, hist.peekLast().kills(), "Round 4 entry has kills=2");
    }

    @Test
    void playerMidMatchJoin_noHistoryContributesZero() throws Exception {
        MatchSessionState state = new MatchSessionState();
        // ct1 has history; newGuy has none (bot or late-join)
        state.playerHistory.put("ct1", new ArrayDeque<>(List.of(new RoundStats(3, 100))));

        String json = "{\"ct1\":{\"team\":\"CT\",\"state\":{\"health\":100,\"round_kills\":0,\"round_totaldmg\":0},\"match_stats\":{\"kills\":0}}," +
                       "\"newGuy\":{\"team\":\"CT\",\"state\":{\"health\":80,\"round_kills\":0,\"round_totaldmg\":0},\"match_stats\":{\"kills\":0}}}";

        int[] p = computePrev3(state, json);
        assertEquals(3,   p[0], "ct1 contributes 3 kills; newGuy contributes 0");
        assertEquals(100, p[1], "ct1 contributes 100 dmg; newGuy contributes 0");
        assertEquals(0,   p[2]);
        assertEquals(0,   p[3]);
    }

    @Test
    void freezetimeOnlyMessages_noOutputRowsEmitted() throws Exception {
        // Verify that saveRoundStart only updates kill snapshots — not history or output.
        MatchSessionState state = new MatchSessionState();
        state.currentRound = 0;

        saveRoundStart(state, ap("ct1", 100, 0, 0, "t1", 100, 0, 0));
        state.currentRound = 1;

        assertEquals(0, state.playerHistory.size(), "saveRoundStart must not touch playerHistory");
        assertEquals(2, state.killsAtRoundStart.size(), "kill snapshots saved for both players");
        assertFalse(state.roundArchived);
    }

    @Test
    void overFollowedByNewRound_roundArchivedCorrectly() throws Exception {
        MatchSessionState state = new MatchSessionState();
        state.currentRound = 1;
        state.killsAtRoundStart.put("ct1", 0);
        state.killsAtRoundStart.put("t1",  0);

        // Over round 1
        archiveRound(state, ap("ct1", 0, 3, 120, "t1", 100, 2, 80));
        state.roundArchived = true;

        assertTrue(state.roundArchived);
        assertEquals(1, state.playerHistory.get("ct1").size());
        assertEquals(3,   state.playerHistory.get("ct1").peekFirst().kills());
        assertEquals(120, state.playerHistory.get("ct1").peekFirst().damage());
        assertEquals(2,   state.playerHistory.get("t1").peekFirst().kills());
        assertEquals(80,  state.playerHistory.get("t1").peekFirst().damage());

        // Freezetime for round 2
        saveRoundStart(state, ap("ct1", 100, 3, 0, "t1", 100, 2, 0));
        state.currentRound = 2;
        state.roundArchived = false;

        assertFalse(state.roundArchived, "roundArchived reset for new round");
        assertEquals(3, state.killsAtRoundStart.get("ct1"), "Kill snapshot updated for round 2");
    }
}
