/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.neo4j.connector.messaging.v1;

import java.util.Map;

import org.neo4j.connector.messaging.AbstractMessageWriter;
import org.neo4j.connector.messaging.MessageEncoder;
import org.neo4j.connector.messaging.ValuePacker;
import org.neo4j.connector.messaging.encode.DiscardAllMessageEncoder;
import org.neo4j.connector.messaging.encode.InitMessageEncoder;
import org.neo4j.connector.messaging.encode.PullAllMessageEncoder;
import org.neo4j.connector.messaging.encode.ResetMessageEncoder;
import org.neo4j.connector.messaging.encode.RunMessageEncoder;
import org.neo4j.connector.messaging.request.DiscardAllMessage;
import org.neo4j.connector.messaging.request.InitMessage;
import org.neo4j.connector.messaging.request.PullAllMessage;
import org.neo4j.connector.messaging.request.ResetMessage;
import org.neo4j.connector.messaging.request.RunMessage;
import org.neo4j.connector.packstream.PackOutput;
import org.neo4j.connector.internal.util.Iterables;

public class MessageWriterV1 extends AbstractMessageWriter
{
    public MessageWriterV1( PackOutput output )
    {
        this( new ValuePackerV1( output ) );
    }

    protected MessageWriterV1( ValuePacker packer )
    {
        super( packer, buildEncoders() );
    }

    private static Map<Byte,MessageEncoder> buildEncoders()
    {
        Map<Byte,MessageEncoder> result = Iterables.newHashMapWithSize( 6 );
        result.put( DiscardAllMessage.SIGNATURE, new DiscardAllMessageEncoder() );
        result.put( InitMessage.SIGNATURE, new InitMessageEncoder() );
        result.put( PullAllMessage.SIGNATURE, new PullAllMessageEncoder() );
        result.put( ResetMessage.SIGNATURE, new ResetMessageEncoder() );
        result.put( RunMessage.SIGNATURE, new RunMessageEncoder() );
        return result;
    }
}
