// Copyright 2020 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.io.binary;

import org.apache.tinkerpop.gremlin.structure.io.Buffer;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryReader;
import org.apache.tinkerpop.gremlin.structure.io.binary.GraphBinaryWriter;
import org.janusgraph.graphdb.relations.RelationIdentifier;

import java.io.IOException;

public class RelationIdentifierGraphBinarySerializer extends JanusGraphTypeSerializer<RelationIdentifier> {
    public RelationIdentifierGraphBinarySerializer() {
        super(GraphBinaryType.RelationIdentifier);
    }

    private void writeString(Buffer buffer, String value) {
        byte[] arr = new byte[value.length()];
        for (int i = 0; i < value.length(); i++) {
            int c = value.charAt(i);
            assert c <= 127;
            byte b = (byte)c;
            if (i+1==value.length()) b |= 0x80; //End marker
            arr[i] = b;
        }
        buffer.writeBytes(arr);
    }

    private String readString(Buffer buffer) {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int c = 0xFF & buffer.readByte();
            sb.append((char)(c & 0x7F));
            if ((c & 0x80) > 0) break;
        }
        return sb.toString();
    }

    @Override
    public RelationIdentifier readNonNullableValue(Buffer buffer, GraphBinaryReader context) throws IOException {
        final byte outVertexIdMarker = buffer.readByte();
        Object outVertexId;
        if (outVertexIdMarker == RelationIdentifier.STRING_MARKER) {
            outVertexId = readString(buffer);
        } else {
            outVertexId = buffer.readLong();
        }
        final long typeId = buffer.readLong();
        final long relationId = buffer.readLong();
        final byte inVertexIdMarker = buffer.readByte();
        Object inVertexId;
        if (inVertexIdMarker == RelationIdentifier.STRING_MARKER) {
            inVertexId = readString(buffer);
        } else {
            inVertexId = buffer.readLong();
        }
        // TODO: will this cause a problem by not passing whether allowStringVertexId is true?
        return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
    }

    @Override
    protected void writeNonNullableValue(RelationIdentifier value, Buffer buffer, GraphBinaryWriter context) throws IOException {
        Object outVertexId = value.getOutVertexId();
        if (outVertexId instanceof Number) {
            buffer.writeByte(RelationIdentifier.LONG_MARKER);
            buffer.writeLong(((Number) outVertexId).longValue());
        } else {
            assert outVertexId instanceof String;
            buffer.writeByte(RelationIdentifier.STRING_MARKER);
            writeString(buffer, (String) outVertexId);
        }
        buffer.writeLong(value.getTypeId());
        buffer.writeLong(value.getRelationId());
        Object inVertexId = value.getInVertexId();
        if (inVertexId instanceof Number) {
            buffer.writeByte(RelationIdentifier.LONG_MARKER);
            buffer.writeLong(((Number) inVertexId).longValue());
        } else {
            assert inVertexId instanceof String;
            buffer.writeByte(RelationIdentifier.STRING_MARKER);
            writeString(buffer, (String) inVertexId);
        }
    }
}
