/*
 * Copyright 2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.data.redis.connection.lettuce;

import static org.hamcrest.collection.IsIterableContainingInOrder.*;
import static org.hamcrest.core.Is.*;
import static org.hamcrest.core.IsEqual.*;
import static org.junit.Assert.*;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands.KeyValue;
import org.springframework.data.redis.test.util.LettuceRedisClientProvider;

import com.lambdaworks.redis.RedisClient;
import com.lambdaworks.redis.api.sync.RedisCommands;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.test.TestSubscriber;

/**
 * @author Christoph Strobl
 */
public class LettuceReactiveStringCommandsTests {

	public static @ClassRule LettuceRedisClientProvider clientProvider = LettuceRedisClientProvider.local();

	static final String KEY_1 = "key-1";
	static final String KEY_2 = "key-2";
	static final String VALUE_2 = "value-2";
	static final String VALUE_1 = "value-1";
	final byte[] KEY_1_BYTES = KEY_1.getBytes(Charset.forName("UTF-8"));
	final byte[] KEY_2_BYTES = KEY_2.getBytes(Charset.forName("UTF-8"));
	final byte[] VALUE_1_BYTES = VALUE_1.getBytes(Charset.forName("UTF-8"));
	final byte[] VALUE_2_BYTES = VALUE_2.getBytes(Charset.forName("UTF-8"));

	LettuceReactiveRedisConnection connection;
	RedisCommands<String, String> commands;

	ByteBuffer key1 = ByteBuffer.wrap(KEY_1_BYTES);
	ByteBuffer value1 = ByteBuffer.wrap(VALUE_1_BYTES);

	ByteBuffer key2 = ByteBuffer.wrap(KEY_2_BYTES);
	ByteBuffer value2 = ByteBuffer.wrap(VALUE_2_BYTES);

	@Before
	public void setUp() {

		RedisClient client = clientProvider.getClient();
		commands = client.connect().sync();
		commands.flushall();

		connection = new LettuceReactiveRedisConnection(client);
	}

	@After
	public void tearDown() {
		commands.close();
		connection.close();
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValueCorrectly() {

		Mono<Boolean> result = connection.stringCommands().set(key1, value1);

		assertThat(result.block(), is(true));
		assertThat(commands.get(KEY_1), is(equalTo(VALUE_1)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void setShouldAddValuesCorrectly() {

		Flux<Boolean> result = connection.stringCommands()
				.set(Flux.fromIterable(Arrays.asList(new KeyValue(key1, value1), new KeyValue(key2, value2))));

		TestSubscriber<Boolean> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		assertThat(commands.get(KEY_1), is(equalTo(VALUE_1)));
		assertThat(commands.get(KEY_2), is(equalTo(VALUE_2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValueCorrectly() {

		commands.set(KEY_1, VALUE_1);

		Mono<ByteBuffer> result = connection.stringCommands().get(key1);
		assertThat(result.block(), is(equalTo(value1)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void getShouldRetriveValuesCorrectly() {

		commands.set(KEY_1, VALUE_1);
		commands.set(KEY_2, VALUE_2);

		Flux<ByteBuffer> result = connection.stringCommands().get(Flux.fromIterable(Arrays.asList(key1, key2)));

		TestSubscriber<ByteBuffer> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertContainValues(new HashSet<>(Arrays.asList(value1, value2)));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValueCorrectly() {

		commands.set(KEY_1, VALUE_1);
		commands.set(KEY_2, VALUE_2);

		Mono<List<ByteBuffer>> result = connection.stringCommands().mGet(Arrays.asList(key1, key2));
		assertThat(result.block(), contains(value1, value2));
	}

	/**
	 * @see DATAREDIS-525
	 */
	@Test
	public void mGetShouldRetriveValuesCorrectly() {

		commands.set(KEY_1, VALUE_1);
		commands.set(KEY_2, VALUE_2);

		Flux<List<ByteBuffer>> result = connection.stringCommands()
				.mGet(Flux.fromIterable(Arrays.asList(Arrays.asList(key1, key2), Arrays.asList(key2))));

		TestSubscriber<List<ByteBuffer>> subscriber = TestSubscriber.create();
		result.subscribe(subscriber);
		subscriber.await();

		subscriber.assertValueCount(2);
		subscriber.assertContainValues(new HashSet<>(Arrays.asList(Arrays.asList(value1, value2), Arrays.asList(value2))));
	}

}
