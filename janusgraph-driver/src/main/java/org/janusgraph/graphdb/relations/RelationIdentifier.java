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
import java.util.Arrays;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public final class RelationIdentifier implements Serializable {

    public static final String TOSTRING_DELIMITER = "-";

    private final Object outVertexId;
    private final long typeId;
    private final long relationId;
    private final Object inVertexId;

    // this does not appear in LongEncoding.BASE_SYMBOLS, so as long as we see this marker,
    // we know the id must be of string type rather than a string-encoded long type
    private static final char STRING_MARKER = 'S';

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

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append(LongEncoding.encode(relationId)).append(TOSTRING_DELIMITER);
        if (outVertexId instanceof Number) {
            s.append(LongEncoding.encode(((Number) outVertexId).longValue()));
        } else {
            assert outVertexId instanceof String;
            s.append(STRING_MARKER).append(outVertexId);
        }
        s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(typeId));
        if (inVertexId != null) {
            if (inVertexId instanceof Number) {
                assert ((Number) inVertexId).longValue() > 0;
                s.append(TOSTRING_DELIMITER).append(LongEncoding.encode(((Number) inVertexId).longValue()));
            } else {
                assert inVertexId instanceof String;
                s.append(TOSTRING_DELIMITER).append(STRING_MARKER).append(inVertexId);
            }
        }
        return s.toString();
    }

    public static RelationIdentifier parse(String id) {
        String[] elements = id.split(TOSTRING_DELIMITER);
        if (elements.length != 3 && elements.length != 4)
            throw new IllegalArgumentException("Not a valid relation identifier: " + id);
        try {
            Object outVertexId;
            if (elements[1].charAt(0) == STRING_MARKER) {
                outVertexId = elements[1].substring(1);
            } else {
                outVertexId = LongEncoding.decode(elements[1]);
            }
            final long typeId = LongEncoding.decode(elements[2]);
            final long relationId = LongEncoding.decode(elements[0]);
            Object inVertexId = null;
            if (elements.length == 4) {
                if (elements[3].charAt(0) == STRING_MARKER) {
                    inVertexId = elements[3].substring(1);
                } else {
                    inVertexId = LongEncoding.decode(elements[3]);
                }
            }
            return new RelationIdentifier(outVertexId, typeId, relationId, inVertexId);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid id - each token expected to be a number", e);
        }
    }
}
