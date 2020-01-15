package com.aerospike.helper.query;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.query.Statement;
import com.aerospike.helper.query.Qualifier.FilterOperation;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static com.aerospike.helper.query.Utils.tryWith;
import static org.assertj.core.api.Assertions.assertThat;

public class UsersTests extends HelperTests{

	private static final String USERS_SET = "users";
	private static final Random RANDOM = new Random();

	@Before
	public void setUp() throws Exception {
		createUsers();
		super.setUp();
	}

	@Test
	public void allUsers() throws IOException {
		tryWith(() -> queryEngine.select(TestQueryEngine.NAMESPACE, USERS_SET, null),
				it -> assertThat(it)
						.hasSize(TestQueryEngine.RECORD_COUNT)
		);
	}

	@Test
	public void usersInterupted() throws IOException {
		try (KeyRecordIterator it = queryEngine.select(TestQueryEngine.NAMESPACE, USERS_SET, null)) {
			int counter = 0;
			while (it.hasNext()) {
				it.next();
				counter++;
				if (counter >= 1000)
					break;
			}
		}
	}
	@Test
	public void usersInNorthRegion() throws IOException {
		Statement stmt = new Statement();
		stmt.setNamespace(TestQueryEngine.NAMESPACE);
		stmt.setSetName(USERS_SET);
		tryWith(() -> queryEngine.select(stmt, new Qualifier("region", FilterOperation.EQ, Value.get("n"))),
				it -> assertThat(it)
						.extracting(rec-> rec.record.getString("region"))
						.containsOnly("n")
		);
	}

	private void createUsers() throws AerospikeException {
		String[] genders = { "m", "f" };
		String[] regions = { "n", "s", "e", "w" };
		String[] randomInterests = { "Music", "Football", "Soccer", "Baseball", "Basketball", "Hockey", "Weekend Warrior", "Hiking", "Camping", "Travel", "Photography"};
		int start = 1;
		int end = TestQueryEngine.RECORD_COUNT;
		/*
		 * see if data is loaded
		 */

		Key itemKey = new Key(TestQueryEngine.NAMESPACE, USERS_SET, "user" + (TestQueryEngine.RECORD_COUNT - 99));
		if (client.exists(null, itemKey)) {
			return;
		}
			for (int j = start; j <= end; j++) {
				// Write user record
				String username = "user" + j;
				Key key = new Key(TestQueryEngine.NAMESPACE, USERS_SET, username);
				Bin bin1 = new Bin("username", "user" + j);
				Bin bin2 = new Bin("password", "pwd" + j);
				Bin bin3 = new Bin("gender", genders[RANDOM.nextInt(2)]);
				Bin bin4 = new Bin("region", regions[RANDOM.nextInt(4)]);
				Bin bin5 = new Bin("lasttweeted", 0);
				Bin bin6 = new Bin("tweetcount", 0);

				int totalInterests = RANDOM.nextInt(7);
				List<String> userInterests = new ArrayList<>();
				for(int i = 0; i < totalInterests; i++) {
					userInterests.add(randomInterests[RANDOM.nextInt(randomInterests.length)]);
				}
				Bin bin7 = new Bin("interests", userInterests);

				client.put(null, key, bin1, bin2, bin3, bin4, bin5, bin6, bin7);
			}
			createTweets();
	}

	private void createTweets() throws AerospikeException {
		String[] randomTweets = {
				"For just $1 you get a half price download of half of the song and listen to it just once.",
				"People tell me my body looks like a melted candle",
				"Come on movie! Make it start!", "Byaaaayy",
				"Please, please, win! Meow, meow, meow!",
				"Put. A. Bird. On. It.",
				"A weekend wasted is a weekend well spent",
				"Would you like to super spike your meal?",
				"We have a mean no-no-bring-bag up here on aisle two.",
				"SEEK: See, Every, EVERY, Kind... of spot",
				"We can order that for you. It will take a year to get there.",
				"If you are pregnant, have a soda.",
				"Hear that snap? Hear that clap?",
				"Follow me and I may follow you",
				"Which is the best cafe in Portland? Discuss...",
				"Portland Coffee is for closers!",
				"Lets get this party started!",
				"How about them portland blazers!", "You got school'd, yo",
				"I love animals", "I love my dog", "What's up Portland",
				"Which is the best cafe in Portland? Discuss...",
				"I dont always tweet, but when I do it is on Tweetaspike" };
		int totalUsers = 10000;
		int maxTweets = 20;
		long ts = 0;

		for (int j = 0; j < totalUsers; j++) {
			// Check if user record exists
			String username = "user" + RANDOM.nextInt(100000);
			Key userKey = new Key(TestQueryEngine.NAMESPACE, USERS_SET, username);
			Record userRecord = client.get(null, userKey);
			if (userRecord != null) {
				// create up to maxTweets random tweets for this user
				int totalTweets = RANDOM.nextInt(maxTweets);
				for (int k = 1; k <= totalTweets; k++) {
					// Create timestamp to store along with the tweet so we can
					// query, index and report on it
					ts = getTimeStamp();
					Key tweetKey = new Key(TestQueryEngine.NAMESPACE, "tweets", username + ":" + k);
					Bin bin1 = new Bin("tweet",
							randomTweets[RANDOM.nextInt(randomTweets.length)]);
					Bin bin2 = new Bin("ts", ts);
					Bin bin3 = new Bin("username", username);

					client.put(null, tweetKey, bin1, bin2, bin3);
				}
				if (totalTweets > 0) {
					// Update tweet count and last tweet'd timestamp in the user
					// record
			        client.put(null, userKey, new Bin("tweetcount", totalTweets), new Bin("lasttweeted", ts));
			        //console.printf("\nINFO: The tweet count now is: " + totalTweets);
				}
			}
		}
	}

	private long getTimeStamp() {
		return System.currentTimeMillis();
	}

}
