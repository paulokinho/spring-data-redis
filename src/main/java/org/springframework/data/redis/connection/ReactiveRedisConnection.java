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
package org.springframework.data.redis.connection;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.function.Supplier;

import org.reactivestreams.Publisher;
import org.springframework.data.redis.connection.RedisStringCommands.SetOption;
import org.springframework.data.redis.core.types.Expiration;
import org.springframework.util.Assert;

import lombok.Data;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * @author Christoph Strobl
 * @since 2.0
 */
public interface ReactiveRedisConnection extends Closeable {

	/**
	 * Get {@link ReactiveKeyCommands}.
	 * 
	 * @return never {@literal null}
	 */
	ReactiveKeyCommands keyCommands();

	/**
	 * Get {@link ReactiveStringCommands}.
	 * 
	 * @return never {@literal null}
	 */
	ReactiveStringCommands stringCommands();

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static interface ReactiveStringCommands {

		/**
		 * Set {@literal value} for {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @return
		 */
		default Mono<Boolean> set(ByteBuffer key, ByteBuffer value) {

			Assert.notNull(key, "Key must not be null!");
			Assert.notNull(value, "Value must not be null!");

			return set(Mono.fromSupplier(() -> new KeyValue(key, value))).next();
		}

		/**
		 * Set each and every {@link KeyValue} item separately.
		 * 
		 * @param values must not be {@literal null}.
		 * @return never {@literal null}.
		 */
		Flux<Boolean> set(Publisher<KeyValue> values);

		/**
		 * Set {@literal value} for {@literal key} with {@literal expiration} and {@literal options}.
		 * 
		 * @param key must not be {@literal null}.
		 * @param value must not be {@literal null}.
		 * @param expiration must not be {@literal null}.
		 * @param option must not be {@literal null}.
		 * @return
		 */
		default Mono<Boolean> set(ByteBuffer key, ByteBuffer value, Expiration expiration, SetOption option) {

			Assert.notNull(key, "Key must not be null!");
			Assert.notNull(value, "Value must not be null!");
			Assert.notNull(expiration, "Expiration must not be null!");
			Assert.notNull(option, "Option must not be null!");

			return set(Mono.fromSupplier(() -> new KeyValue(key, value)), () -> expiration, () -> option).next();
		}

		/**
		 * Set {@literal value} for {@literal key} with {@literal expiration} and {@literal options} one by one.
		 * 
		 * @param values must not be {@literal null}.
		 * @param expiration must not be {@literal null}.
		 * @param option must not be {@literal null}.
		 * @return
		 */
		Flux<Boolean> set(Publisher<KeyValue> values, Supplier<Expiration> expiration, Supplier<SetOption> option);

		/**
		 * Get single element stored at {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @return
		 */
		default Mono<ByteBuffer> get(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");
			return get(Mono.fromSupplier(() -> key)).next();
		}

		/**
		 * Get elements one by one.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		Flux<ByteBuffer> get(Publisher<ByteBuffer> keys);

		/**
		 * Get multiple values in one batch.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		default Mono<List<ByteBuffer>> mGet(List<ByteBuffer> keys) {

			Assert.notNull(keys, "Keys must not be null!");
			return mGet(Mono.fromSupplier(() -> keys)).next();
		}

		/**
		 * Get multiple values at in batches.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		Flux<List<ByteBuffer>> mGet(Publisher<Collection<ByteBuffer>> keysets);

		/**
		 * @author Christoph Strobl
		 * @since 2.0
		 */
		@Data
		public static class KeyValue {

			final ByteBuffer key;
			final ByteBuffer value;

			public byte[] keyAsBytes() {
				return key.array();
			}

			public byte[] valueAsBytes() {
				return value.array();
			}
		}
	}

	/**
	 * @author Christoph Strobl
	 * @since 2.0
	 */
	static interface ReactiveKeyCommands {

		/**
		 * Delete {@literal key}.
		 * 
		 * @param key must not be {@literal null}.
		 * @return
		 */
		default Mono<Long> del(ByteBuffer key) {

			Assert.notNull(key, "Key must not be null!");
			return del(Mono.fromSupplier(() -> key)).next();
		}

		/**
		 * Delete {@literal keys} one by one.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		Flux<Long> del(Publisher<ByteBuffer> keys);

		/**
		 * Delete {@literal keys} one in one batch.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		default Mono<Long> mDel(List<ByteBuffer> keys) {
			return mDel(Mono.fromSupplier(() -> keys)).next();
		}

		/**
		 * Delete {@literal keys} in batches.
		 * 
		 * @param keys must not be {@literal null}.
		 * @return
		 */
		Flux<Long> mDel(Publisher<List<ByteBuffer>> keys);
	}
}
