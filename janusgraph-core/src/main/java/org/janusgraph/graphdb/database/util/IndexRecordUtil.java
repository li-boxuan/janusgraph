// Copyright 2022 JanusGraph Authors
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

package org.janusgraph.graphdb.database.util;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.ReadBuffer;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.StandardKeyInformation;
import org.janusgraph.diskstorage.util.HashingUtil;
import org.janusgraph.diskstorage.util.StaticArrayEntry;
import org.janusgraph.graphdb.database.IndexRecordEntry;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.database.idhandling.IDHandler;
import org.janusgraph.graphdb.database.idhandling.VariableLong;
import org.janusgraph.graphdb.database.index.IndexMutationType;
import org.janusgraph.graphdb.database.index.IndexRecords;
import org.janusgraph.graphdb.database.index.IndexUpdate;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.database.serialize.DataOutput;
import org.janusgraph.graphdb.database.serialize.InternalAttributeUtil;
import org.janusgraph.graphdb.database.serialize.Serializer;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.query.vertex.VertexCentricQueryBuilder;
import org.janusgraph.graphdb.relations.RelationIdentifier;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.ParameterIndexField;
import org.janusgraph.graphdb.types.ParameterType;
import org.janusgraph.util.encoding.LongEncoding;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IndexRecordUtil {

    public static final IndexAppliesToFunction FULL_INDEX_APPLIES_TO_FILTER = IndexRecordUtil::indexAppliesTo;
    public static final IndexAppliesToFunction INDEX_APPLIES_TO_NO_CONSTRAINTS_FILTER = IndexRecordUtil::indexAppliesToWithoutConstraints;

    private static final int DEFAULT_OBJECT_BYTELEN = 30;
    private static final byte FIRST_INDEX_COLUMN_BYTE = 0;

    private static final char LONG_ID_PREFIX = 'L';
    private static final char STRING_ID_PREFIX = 'S';
    private static final char RELATION_ID_PREFIX = 'R';

    public static Object[] getValues(IndexRecordEntry[] record) {
        final Object[] values = new Object[record.length];
        for (int i = 0; i < values.length; i++) {
            values[i]=record[i].getValue();
        }
        return values;
    }

    public static MixedIndexType getMixedIndex(String indexName, StandardJanusGraphTx transaction) {
        final IndexType index = ManagementSystem.getGraphIndexDirect(indexName, transaction);
        Preconditions.checkArgument(index!=null,"Index with name [%s] is unknown or not configured properly",indexName);
        Preconditions.checkArgument(index.isMixedIndex());
        return (MixedIndexType)index;
    }

    public static String element2String(JanusGraphElement element, boolean allowStringVertexId) {
        return element2String(element.id(), allowStringVertexId);
    }

    /**
     * Convert an element's (including vertex and relation) id into a String
     *
     * If allowStringVertexId is enabled, we add a one character prefix as identifier to differentiate different types
     * Otherwise, we don't add any special marker
     * @param elementId
     * @param allowStringVertexId
     * @return
     */
    public static String element2String(Object elementId, boolean allowStringVertexId) {
        Preconditions.checkArgument(elementId instanceof Long || elementId instanceof RelationIdentifier || elementId instanceof String);
        if (allowStringVertexId) {
            if (elementId instanceof Long) {
                return LONG_ID_PREFIX + longID2Name((Long)elementId);
            } else if (elementId instanceof RelationIdentifier) {
                return RELATION_ID_PREFIX + ((RelationIdentifier) elementId).toString();
            } else {
                return STRING_ID_PREFIX + (String) elementId;
            }
        } else {
            if (elementId instanceof Long) {
                return longID2Name((Long)elementId);
            } else {
                return ((RelationIdentifier) elementId).toString();
            }
        }
    }

    public static Object string2ElementId(String str, boolean allowStringVertexId) {
        if (StringUtils.isEmpty(str)) {
            throw new IllegalArgumentException("Empty string cannot be converted to a valid id");
        }
        if (allowStringVertexId) {
            if (str.charAt(0) == LONG_ID_PREFIX) {
                return name2LongID(str.substring(1));
            } else if (str.charAt(0) == RELATION_ID_PREFIX) {
                return RelationIdentifier.parse(str.substring(1));
            } else if (str.charAt(0) == STRING_ID_PREFIX) {
                return str.substring(1);
            } else {
                throw new IllegalArgumentException("String is not a representation of a valid id: " + str);
            }
        } else {
            if (str.contains(RelationIdentifier.TOSTRING_DELIMITER)) {
                return RelationIdentifier.parse(str);
            } else {
                return name2LongID(str);
            }
        }
    }

    public static String key2Field(MixedIndexType index, PropertyKey key) {
        return key2Field(index.getField(key));
    }

    public static String key2Field(ParameterIndexField field) {
        assert field!=null;
        return ParameterType.MAPPED_NAME.findParameter(field.getParameters(),keyID2Name(field.getFieldKey()));
    }

    public static String keyID2Name(PropertyKey key) {
        return longID2Name(key.longId());
    }

    public static String longID2Name(long id) {
        Preconditions.checkArgument(id > 0);
        return LongEncoding.encode(id);
    }

    public static long name2LongID(String name) {
        return LongEncoding.decode(name);
    }

    public static RelationIdentifier bytebuffer2RelationId(ReadBuffer b, boolean allowStringVertexId) {
        Object[] relationId = new Object[4];
        relationId[0] = VariableLong.readPositive(b);
        relationId[1] = IDHandler.readVertexId(b, true, allowStringVertexId);
        relationId[2] = VariableLong.readPositive(b);
        if (b.hasRemaining()) {
            relationId[3] = IDHandler.readVertexId(b, true, allowStringVertexId);
        } else {
            relationId = Arrays.copyOfRange(relationId,0,3);
        }
        return RelationIdentifier.get(relationId);
    }

    public static StandardKeyInformation getKeyInformation(final ParameterIndexField field) {
        return new StandardKeyInformation(field.getFieldKey(),field.getParameters());
    }

    public static IndexMutationType getUpdateType(InternalRelation relation) {
        assert relation.isNew() || relation.isRemoved();
        return (relation.isNew()? IndexMutationType.ADD : IndexMutationType.DELETE);
    }

    public static boolean indexAppliesTo(IndexType index, JanusGraphElement element) {
        return indexAppliesToWithoutConstraints(index, element) && indexMatchesConstraints(index, element);
    }

    public static boolean indexAppliesToWithoutConstraints(IndexType index, JanusGraphElement element) {
        return index.getElement().isInstance(element) &&
            (!(index instanceof CompositeIndexType) || ((CompositeIndexType)index).getStatus()!= SchemaStatus.DISABLED);
    }

    public static boolean indexMatchesConstraints(IndexType index, JanusGraphElement element) {
        return !index.hasSchemaTypeConstraint() ||
            index.getElement().matchesConstraint(index.getSchemaTypeConstraint(),element);
    }

    public static PropertyKey[] getKeysOfRecords(IndexRecordEntry[] record) {
        final PropertyKey[] keys = new PropertyKey[record.length];
        for (int i=0;i<record.length;i++) keys[i]=record[i].getKey();
        return keys;
    }

    public static int getIndexTTL(InternalVertex vertex, PropertyKey... keys) {
        int ttl = StandardJanusGraph.getTTL(vertex);
        for (final PropertyKey key : keys) {
            final int kttl = ((InternalRelationType) key).getTTL();
            if (kttl > 0 && (kttl < ttl || ttl <= 0)) ttl = kttl;
        }
        return ttl;
    }

    public static IndexRecordEntry[] indexMatch(JanusGraphRelation relation, CompositeIndexType index) {
        final IndexField[] fields = index.getFieldKeys();
        final IndexRecordEntry[] match = new IndexRecordEntry[fields.length];
        for (int i = 0; i <fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = relation.valueOrNull(f.getFieldKey());
            if (value==null) return null; //No match
            match[i] = new IndexRecordEntry(relation.longId(),value,f.getFieldKey());
        }
        return match;
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index) {
        return indexMatches(vertex,index,null,null);
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                            PropertyKey replaceKey, Object replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        if (indexAppliesTo(index,vertex)) {
            indexMatches(vertex,new IndexRecordEntry[fields.length],matches,fields,0,false,
                replaceKey,new IndexRecordEntry(0,replaceValue,replaceKey));
        }
        return matches;
    }

    public static IndexRecords indexMatches(JanusGraphVertex vertex, CompositeIndexType index,
                                             boolean onlyLoaded, PropertyKey replaceKey, IndexRecordEntry replaceValue) {
        final IndexRecords matches = new IndexRecords();
        final IndexField[] fields = index.getFieldKeys();
        indexMatches(vertex,new IndexRecordEntry[fields.length],matches,fields,0,onlyLoaded,replaceKey,replaceValue);
        return matches;
    }

    public static void indexMatches(JanusGraphVertex vertex, IndexRecordEntry[] current, IndexRecords matches,
                                     IndexField[] fields, int pos,
                                     boolean onlyLoaded, PropertyKey replaceKey, IndexRecordEntry replaceValue) {
        if (pos>= fields.length) {
            matches.add(current);
            return;
        }

        final PropertyKey key = fields[pos].getFieldKey();

        List<IndexRecordEntry> values;
        if (key.equals(replaceKey)) {
            values = ImmutableList.of(replaceValue);
        } else {
            values = new ArrayList<>();
            Iterable<JanusGraphVertexProperty> props;
            if (onlyLoaded ||
                (!vertex.isNew() && IDManager.VertexIDType.PartitionedVertex.is(vertex.id()))) {
                //going through transaction so we can query deleted vertices
                final VertexCentricQueryBuilder qb = ((InternalVertex)vertex).tx().query(vertex);
                qb.noPartitionRestriction().type(key);
                if (onlyLoaded) qb.queryOnlyLoaded();
                props = qb.properties();
            } else {
                props = vertex.query().keys(key.name()).properties();
            }
            for (final JanusGraphVertexProperty p : props) {
                assert !onlyLoaded || p.isLoaded() || p.isRemoved();
                assert key.dataType().equals(p.value().getClass()) : key + " -> " + p;
                values.add(new IndexRecordEntry(p));
            }
        }
        for (final IndexRecordEntry value : values) {
            current[pos]=value;
            indexMatches(vertex,current,matches,fields,pos+1,onlyLoaded,replaceKey,replaceValue);
        }
    }


    private static Entry getIndexEntry(CompositeIndexType index, IndexRecordEntry[] record,
                                       JanusGraphElement element, Serializer serializer, boolean allowStringVertexId) {
        final DataOutput out = serializer.getDataOutput(1+8+8*record.length+4*8);
        out.putByte(FIRST_INDEX_COLUMN_BYTE);
        if (index.getCardinality()!=Cardinality.SINGLE) {
            if (element instanceof JanusGraphVertex) {
                IDHandler.writeVertexId(out, element.id(), true, allowStringVertexId);
            } else {
                assert element instanceof JanusGraphRelation;
                assert ((JanusGraphRelation) element).longId() == ((RelationIdentifier) element.id()).getRelationId();
                VariableLong.writePositive(out, ((JanusGraphRelation) element).longId());
            }
            if (index.getCardinality()!=Cardinality.SET) {
                for (final IndexRecordEntry re : record) {
                    VariableLong.writePositive(out, re.getRelationId());
                }
            }
        }
        final int valuePosition=out.getPosition();
        if (element instanceof JanusGraphVertex) {
            IDHandler.writeVertexId(out, element.id(), true, allowStringVertexId);
        } else {
            assert element instanceof JanusGraphRelation;
            final RelationIdentifier rid = (RelationIdentifier)element.id();
            VariableLong.writePositive(out, rid.getRelationId());
            IDHandler.writeVertexId(out, rid.getOutVertexId(), true, allowStringVertexId);
            VariableLong.writePositive(out, rid.getTypeId());
            if (rid.getInVertexId() != null) {
                IDHandler.writeVertexId(out, rid.getInVertexId(), true, allowStringVertexId);
            }
        }
        return new StaticArrayEntry(out.getStaticBuffer(),valuePosition);
    }

    public static StaticBuffer getIndexKey(CompositeIndexType index, IndexRecordEntry[] record, Serializer serializer, boolean hashKeys, HashingUtil.HashLength hashLength) {
        return getIndexKey(index, IndexRecordUtil.getValues(record), serializer, hashKeys, hashLength);
    }

    public static StaticBuffer getIndexKey(CompositeIndexType index, Object[] values, Serializer serializer, boolean hashKeys, HashingUtil.HashLength hashLength) {
        final DataOutput out = serializer.getDataOutput(8*DEFAULT_OBJECT_BYTELEN + 8);
        VariableLong.writePositive(out, index.longId());
        final IndexField[] fields = index.getFieldKeys();
        Preconditions.checkArgument(fields.length>0 && fields.length==values.length);
        for (int i = 0; i < fields.length; i++) {
            final IndexField f = fields[i];
            final Object value = values[i];
            Preconditions.checkNotNull(value);
            if (InternalAttributeUtil.hasGenericDataType(f.getFieldKey())) {
                out.writeClassAndObject(value);
            } else {
                assert value.getClass().equals(f.getFieldKey().dataType()) : value.getClass() + " - " + f.getFieldKey().dataType();
                out.writeObjectNotNull(value);
            }
        }
        StaticBuffer key = out.getStaticBuffer();
        if (hashKeys) key = HashingUtil.hashPrefixKey(hashLength,key);
        return key;
    }

    public static long getIndexIdFromKey(StaticBuffer key, boolean hashKeys, HashingUtil.HashLength hashLength) {
        if (hashKeys) key = HashingUtil.getKey(hashLength,key);
        return VariableLong.readPositive(key.asReadBuffer());
    }

    public static IndexUpdate<StaticBuffer, Entry> getCompositeIndexUpdate(CompositeIndexType index, IndexMutationType indexMutationType, IndexRecordEntry[] record,
                                                                           JanusGraphElement element, Serializer serializer, boolean hashKeys, HashingUtil.HashLength hashLength,
                                                                           boolean allowStringVertexId){
        return new IndexUpdate<>(index, indexMutationType,
            getIndexKey(index, record, serializer, hashKeys, hashLength),
            getIndexEntry(index, record, element, serializer, allowStringVertexId), element);
    }

    public static IndexUpdate<String, IndexEntry> getMixedIndexUpdate(JanusGraphElement element, PropertyKey key, Object value,
                                                                      MixedIndexType index, IndexMutationType updateType, boolean allowStringVertexId)  {
        return new IndexUpdate<>(index, updateType, element2String(element, allowStringVertexId), new IndexEntry(key2Field(index.getField(key)), value), element);
    }
}
