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

import java.nio.ByteBuffer;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.reactivestreams.Publisher;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.ReactiveRedisConnection.BooleanResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ByteBufferResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.KeyValue;
import org.springframework.data.redis.connection.ReactiveRedisConnection.MultiValueResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands;
import org.springframework.data.redis.connection.RedisStringCommands.BitOperation;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import com.lambdaworks.redis.SetArgs;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveStringCommands implements ReactiveStringCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 * 
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveStringCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mGet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<MultiValueResponse<List<ByteBuffer>, ByteBuffer>> mGet(Publisher<List<ByteBuffer>> keyCollections) {

		return connection.execute(cmd -> {

			return Flux.from(keyCollections).flatMap((keys) -> {

				return LettuceReactiveRedisConnection.<MultiValueResponse<List<ByteBuffer>, ByteBuffer>> monoConverter()
						.convert(cmd
								.mget(
										keys.stream().map(ByteBuffer::array).collect(Collectors.toList()).toArray(new byte[keys.size()][]))
								.map((value) -> value != null ? ByteBuffer.wrap(value) : ByteBuffer.allocate(0)).toList()
								.map((values) -> new MultiValueResponse<>(keys, values)));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#set(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<KeyValue>> set(Publisher<KeyValue> values) {

		return connection.execute(cmd -> {
			return Flux.from(values).flatMap((kv) -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.set(kv.getKey().array(), kv.getValue().array()).map(LettuceConverters::stringToBoolean))
						.map((value) -> new BooleanResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<KeyValue>> getSet(Publisher<KeyValue> values) {

		return connection.execute(cmd -> {
			return Flux.from(values).flatMap((kv) -> {

				return LettuceReactiveRedisConnection.<ByteBufferResponse<KeyValue>> monoConverter()
						.convert(cmd.getset(kv.getKey().array(), kv.getValue().array())
								.map((value) -> new ByteBufferResponse<>(kv, ByteBuffer.wrap(value)))
								.defaultIfEmpty(new ByteBufferResponse<>(kv, ByteBuffer.allocate(0))));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#set(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<KeyValue>> set(Publisher<KeyValue> values, Supplier<Expiration> expiration,
			Supplier<SetOption> option) {

		return connection.execute(cmd -> {

			return Flux.from(values).flatMap((kv) -> {

				SetArgs args = LettuceConverters.toSetArgs(expiration.get(), option.get());

				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.set(kv.getKey().array(), kv.getValue().array(), args).map(LettuceConverters::stringToBoolean))
						.map((value) -> new BooleanResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#get(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<ByteBufferResponse<ByteBuffer>> get(Publisher<ByteBuffer> keys) {

		return connection.execute(cmd -> {
			return Flux.from(keys).flatMap((key) -> {

				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.get(key.array()).map(ByteBuffer::wrap)).map((value) -> new ByteBufferResponse<>(key, value))
						.defaultIfEmpty(new ByteBufferResponse<>(key, ByteBuffer.allocate(0)));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setNX(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<KeyValue>> setNX(Publisher<KeyValue> values) {

		return connection.execute(cmd -> {

			return Flux.from(values).flatMap(kv -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.setnx(kv.keyAsBytes(), kv.valueAsBytes()))
						.map((value) -> new BooleanResponse<KeyValue>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setEX(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<KeyValue>> setEX(Publisher<KeyValue> source, Supplier<Expiration> expireTimeout) {
		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(kv -> {

				return LettuceReactiveRedisConnection.<String> monoConverter()
						.convert(cmd.setex(kv.keyAsBytes(), expireTimeout.get().getExpirationTimeInSeconds(), kv.valueAsBytes()))
						.map(LettuceConverters::stringToBoolean).map((value) -> new BooleanResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#pSetEX(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<KeyValue>> pSetEX(Publisher<KeyValue> source, Supplier<Expiration> expireTimeout) {

		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(kv -> {

				return LettuceReactiveRedisConnection.<String> monoConverter()
						.convert(
								cmd.psetex(kv.keyAsBytes(), expireTimeout.get().getExpirationTimeInMilliseconds(), kv.valueAsBytes()))
						.map(LettuceConverters::stringToBoolean).map((value) -> new BooleanResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<List<KeyValue>>> mSet(Publisher<List<KeyValue>> source) {

		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(values -> {

				Map<byte[], byte[]> map = new LinkedHashMap<>();
				values.forEach(kv -> map.put(kv.keyAsBytes(), kv.valueAsBytes()));

				return LettuceReactiveRedisConnection.<String> monoConverter().convert(cmd.mset(map))
						.map(LettuceConverters::stringToBoolean).map((value) -> new BooleanResponse<>(values, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#mSet(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<BooleanResponse<List<KeyValue>>> mSetNX(Publisher<List<KeyValue>> source) {

		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(values -> {

				Map<byte[], byte[]> map = new LinkedHashMap<>();
				values.forEach(kv -> map.put(kv.keyAsBytes(), kv.valueAsBytes()));

				return LettuceReactiveRedisConnection.<Boolean> monoConverter().convert(cmd.msetnx(map))
						.map((value) -> new BooleanResponse<>(values, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#append(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<KeyValue, Long>> append(Publisher<KeyValue> source) {

		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(kv -> {

				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.append(kv.keyAsBytes(), kv.valueAsBytes())).map((value) -> new NumericResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getRange(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<ByteBufferResponse<ByteBuffer>> getRange(Publisher<ByteBuffer> keys, Supplier<Long> begin,
			Supplier<Long> end) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {
				return LettuceReactiveRedisConnection.<ByteBuffer> monoConverter()
						.convert(cmd.getrange(key.array(), begin.get(), end.get()).map(ByteBuffer::wrap))
						.map((value) -> new ByteBufferResponse<>(key, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setRange(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<NumericResponse<KeyValue, Long>> setRange(Publisher<KeyValue> keys, Supplier<Long> offset) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(kv -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.setrange(kv.keyAsBytes(), offset.get(), kv.valueAsBytes()))
						.map((value) -> new NumericResponse<>(kv, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#getBit(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<ByteBuffer>> getBit(Publisher<ByteBuffer> keys, Supplier<Long> offset) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.getbit(key.array(), offset.get()).map(LettuceConverters::toBoolean))
						.map(value -> new BooleanResponse<>(key, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#setBit(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<BooleanResponse<ByteBuffer>> setBit(Publisher<ByteBuffer> keys, Supplier<Long> offset,
			Supplier<Boolean> value) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {
				return LettuceReactiveRedisConnection.<Boolean> monoConverter()
						.convert(cmd.setbit(key.array(), offset.get(), value.get() ? 1 : 0).map(LettuceConverters::toBoolean))
						.map(respValue -> new BooleanResponse<>(key, respValue));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitCount(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ByteBuffer, Long>> bitCount(Publisher<ByteBuffer> source) {

		return connection.execute(cmd -> {

			return Flux.from(source).flatMap(value -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.bitcount(value.array()))
						.map(responseValue -> new NumericResponse<>(value, responseValue));
			});
		});

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitCount(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<NumericResponse<ByteBuffer, Long>> bitCount(Publisher<ByteBuffer> keys, Supplier<Range<Long>> range) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {

				Range<Long> rangeToUse = range.get();
				return LettuceReactiveRedisConnection.<Long> monoConverter()
						.convert(cmd.bitcount(key.array(), rangeToUse.getLowerBound(), rangeToUse.getUpperBound()))
						.map(value -> new NumericResponse<>(key, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveStringCommands#bitOp(org.reactivestreams.Publisher, java.util.function.Supplier, java.util.function.Supplier)
	 */
	@Override
	public Flux<NumericResponse<List<ByteBuffer>, Long>> bitOp(Publisher<List<ByteBuffer>> keys,
			Supplier<BitOperation> bitOp, Supplier<ByteBuffer> destination) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {

				Observable<Long> result = null;
				byte[] destinationKey = destination.get().array();
				byte[][] sourceKeys = key.stream().map(ByteBuffer::array).toArray(size -> new byte[size][]);

				switch (bitOp.get()) {
					case AND:
						result = cmd.bitopAnd(destinationKey, sourceKeys);
						break;
					case OR:
						result = cmd.bitopOr(destinationKey, sourceKeys);
						break;
					case XOR:
						result = cmd.bitopXor(destinationKey, sourceKeys);
						break;
					case NOT:

						Assert.isTrue(sourceKeys.length == 1, "BITOP NOT does not allow more than 1 source key.");

						result = cmd.bitopNot(destinationKey, sourceKeys[0]);
						break;

				}

				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(result)
						.map(value -> new NumericResponse<>(key, value));
			});
		});
	}
}
