/*
 * Copyright 2011-2013 the original author or authors.
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

import com.lambdaworks.redis.codec.ByteArrayCodec;
import com.lambdaworks.redis.codec.RedisCodec;

/**
 * Basic codec that returns the raw data as byte[].
 * 
 * @author Costin Leau
 */
class BytesRedisCodec implements RedisCodec<byte[], byte[]> {

	ByteArrayCodec delegate = new ByteArrayCodec();

	public byte[] decodeKey(ByteBuffer bytes) {
		return delegate.decodeKey(bytes);
	}

	public byte[] decodeValue(ByteBuffer bytes) {
		return delegate.decodeValue(bytes);
	}

	public ByteBuffer encodeKey(byte[] key) {
		return delegate.encodeKey(key);
	}

	public ByteBuffer encodeValue(byte[] value) {
		return delegate.encodeValue(value);
	}
}
