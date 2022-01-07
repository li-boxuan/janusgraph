// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.graphdb.relations;

import org.janusgraph.util.encoding.LongEncoding;

import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    public static final String TOSTRING_DELIMITER = "-";
    /**
     * this is a marker to represent that the id is serialized in a byte
     * representation (encoded by base64). The marker MUST not use any
     * character that might appear in {@link LongEncoding} encoder, so
     * that we can distinguish the byte encoding from LongEncoding
     */
    public static final String BYTE_REPRESENTATION_MARKER = "B";

    public static final byte LONG_MARKER = 0;
    public static final byte STRING_MARKER = 1;
    public static final byte NULL_MARKER = 2;

    private final Object outVertexId;
    private final long typeId;
    private final long relationId;
    private final Object inVertexId;

    private RelationIdentifier() {
        outVertexId = null;
        typeId = 0;
        relationId = 0;
        inVertexId = null;
    }

    public RelationIdentifier(final Object outVertexId, final long typeId, final long relationId, final Object inVertexId) {
        this.outVertexId = outVertexId;
        this.typeId = typeId;
        this.relationId = relationId;
        this.inVertexId = inVertexId;
    }

    public long getRelationId() {
        return relationId;
    }

    public long getTypeId() {
        return typeId;
    }

    public Object getOutVertexId() {
        return outVertexId;
    }

    public Object getInVertexId() {
        return inVertexId;
    }

    public static RelationIdentifier get(Object[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] instanceof Number && (long) ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], (long) ids[2], (long) ids[0], ids.length == 4 ? ids[3] : 0);
    }

    public static RelationIdentifier get(int[] ids) {
        if (ids.length != 3 && ids.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        for (int i = 0; i < 3; i++) {
            if (ids[i] < 0)
                throw new IllegalArgumentException("Not a valid relation identifier: " + Arrays.toString(ids));
        }
        return new RelationIdentifier(ids[1], ids[2], ids[0], ids.length == 4 ? ids[3] : 0);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(relationId);
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (!getClass().isInstance(other)) return false;
        RelationIdentifier oth = (RelationIdentifier) other;
        return relationId == oth.relationId && typeId == oth.typeId;
    }

    private int getByteBufSize() {
        int ans = 18; // relationId (8) + typeId (8) + marker for outVertexId (1) + marker for inVertexId (1)
        ans += outVertexId instanceof Number ? 8 : ((String) outVertexId).length();
        if (inVertexId != null) {
            ans += inVertexId instanceof Number ? 8 : ((String) inVertexId).length();
        }
        return ans;
    }

    private static void writeId(ByteBuffer buf, Object id) {
        if (id == null) {
            buf.put(NULL_MARKER);
        } else if (id instanceof Number) {
            buf.put(LONG_MARKER);
            buf.putLong((long) id);
        } else {
            buf.put(STRING_MARKER);
            final String sId = (String) id;
            for (int i = 0; i < sId.length(); i++) {
                int c = sId.charAt(i);
                assert c <= 127;
                byte b = (byte)c;
                if (i+1==sId.length()) b |= 0x80; //End marker
                buf.put(b);
            }
        }
    }

    private static Object readId(ByteBuffer buf) {
        final byte marker = buf.get();
        if (marker == NULL_MARKER) {
            return null;
        } else if (marker == LONG_MARKER) {
            return buf.getLong();
        } else {
            assert marker == STRING_MARKER;
            StringBuilder sb = new StringBuilder();
            while (true) {
                int c = 0xFF & buf.get();
                sb.append((char)(c & 0x7F));
                if ((c & 0x80) > 0) break;
            }
            return sb.toString();
        }
    }

    @Override
    public String toString() {
        if (outVertexId instanceof String || inVertexId instanceof String) {
            ByteBuffer buf = ByteBuffer.allocate(getByteBufSize());
            buf.putLong(relationId);
            writeId(buf, outVertexId);
            buf.putLong(typeId);
            writeId(buf, inVertexId);
            return BYTE_REPRESENTATION_MARKER + Base64.getEncoder().encodeToString(buf.array());
        } else {
            StringBuilder s = new StringBuilder();
            s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER).append(LongEncoding.encode(((Number) outVertexId).longValue()))
                .append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
            if (inVertexId != null && ((Number) inVertexId).longValue() > 0) {
                s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(((Number) inVertexId).longValue()));
            }
            return s.toString();
        }
    }

    public static RelationIdentifier parse(String id) {
        if (id.startsWith(BYTE_REPRESENTATION_MARKER)) {
            ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(id.substring(BYTE_REPRESENTATION_MARKER.length())));
            long relationId = buf.getLong();
            Object outVertexId = readId(buf);
            long typeId = buf.getLong();
            Object inVertexId = readId(buf);
            RelationIdentifier rId = new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
            return rId;
        } else {
            String[] elements = id.split(TOSTRING_DELIMITER);
            if (elements.length != 3 && elements.length != 4)
                throw new IllegalArgumentException("Not a valid relation identifier: " + id);
            try {
                return new RelationIdentifier(LongEncoding.decode(elements[1]),
                    LongEncoding.decode(elements[2]),
                    LongEncoding.decode(elements[0]),
                    elements.length == 4 ? LongEncoding.decode(elements[3]) : 0);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
            }
        }
    }
}
