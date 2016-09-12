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
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.ReactiveRedisConnection.NumericResponse;
import org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands;
import org.springframework.util.Assert;
import org.springframework.util.NumberUtils;

import reactor.core.publisher.Flux;
import rx.Observable;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public class LettuceReactiveNumberCommands implements ReactiveNumberCommands {

	private final LettuceReactiveRedisConnection connection;

	/**
	 * Create new {@link LettuceReactiveStringCommands}.
	 * 
	 * @param connection must not be {@literal null}.
	 */
	public LettuceReactiveNumberCommands(LettuceReactiveRedisConnection connection) {

		Assert.notNull(connection, "Connection must not be null!");
		this.connection = connection;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#incr(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ByteBuffer, Long>> incr(Publisher<ByteBuffer> keys) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.incr(key.array()))
						.map(value -> new NumericResponse<>(key, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#incrBy(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public <T extends Number> Flux<NumericResponse<ByteBuffer, T>> incrBy(Publisher<ByteBuffer> keys, Supplier<T> value) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {

				T incrBy = value.get();

				Assert.notNull(incrBy, "Value for INCRBY must not be null.");

				Observable<? extends Number> result = null;
				if (incrBy instanceof Double || incrBy instanceof Float) {
					result = cmd.incrbyfloat(key.array(), incrBy.doubleValue());
				} else {
					result = cmd.incrby(key.array(), incrBy.longValue());
				}

				return LettuceReactiveRedisConnection.<T> monoConverter()
						.convert(result.map(val -> NumberUtils.convertNumberToTargetClass(val, incrBy.getClass())))
						.map(res -> new NumericResponse<>(key, res));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#decr(org.reactivestreams.Publisher)
	 */
	@Override
	public Flux<NumericResponse<ByteBuffer, Long>> decr(Publisher<ByteBuffer> keys) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {
				return LettuceReactiveRedisConnection.<Long> monoConverter().convert(cmd.decr(key.array()))
						.map(value -> new NumericResponse<>(key, value));
			});
		});
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.redis.connection.ReactiveRedisConnection.ReactiveNumberCommands#decrBy(org.reactivestreams.Publisher, java.util.function.Supplier)
	 */
	@Override
	public <T extends Number> Flux<NumericResponse<ByteBuffer, T>> decrBy(Publisher<ByteBuffer> keys, Supplier<T> value) {

		return connection.execute(cmd -> {

			return Flux.from(keys).flatMap(key -> {

				T decrBy = value.get();

				Assert.notNull(decrBy, "Value for DECRBY must not be null.");

				Observable<? extends Number> result = null;
				if (decrBy instanceof Double || decrBy instanceof Float) {
					result = cmd.incrbyfloat(key.array(), decrBy.doubleValue() * (-1.0D));
				} else {
					result = cmd.decrby(key.array(), decrBy.longValue());
				}

				return LettuceReactiveRedisConnection.<T> monoConverter()
						.convert(result.map(val -> NumberUtils.convertNumberToTargetClass(val, decrBy.getClass())))
						.map(res -> new NumericResponse<>(key, res));
			});
		});
	}

}
