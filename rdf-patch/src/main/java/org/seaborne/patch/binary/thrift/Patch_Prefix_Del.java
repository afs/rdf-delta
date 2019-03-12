/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  See the NOTICE file distributed with this work for additional
 *  information regarding copyright ownership.
 */

package org.seaborne.patch.binary.thrift;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("all")
public class Patch_Prefix_Del implements org.apache.thrift.TBase<Patch_Prefix_Del, Patch_Prefix_Del._Fields>, java.io.Serializable, Cloneable, Comparable<Patch_Prefix_Del> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("Patch_Prefix_Del");

  private static final org.apache.thrift.protocol.TField GRAPH_NODE_FIELD_DESC = new org.apache.thrift.protocol.TField("graphNode", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField PREFIX_FIELD_DESC = new org.apache.thrift.protocol.TField("prefix", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new Patch_Prefix_DelStandardSchemeFactory());
    schemes.put(TupleScheme.class, new Patch_Prefix_DelTupleSchemeFactory());
  }

  public RDF_Term graphNode; // optional
  public String prefix; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    GRAPH_NODE((short)1, "graphNode"),
    PREFIX((short)2, "prefix");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // GRAPH_NODE
          return GRAPH_NODE;
        case 2: // PREFIX
          return PREFIX;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.GRAPH_NODE};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.GRAPH_NODE, new org.apache.thrift.meta_data.FieldMetaData("graphNode", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_Term.class)));
    tmpMap.put(_Fields.PREFIX, new org.apache.thrift.meta_data.FieldMetaData("prefix", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(Patch_Prefix_Del.class, metaDataMap);
  }

  public Patch_Prefix_Del() {
  }

  public Patch_Prefix_Del(
    String prefix)
  {
    this();
    this.prefix = prefix;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public Patch_Prefix_Del(Patch_Prefix_Del other) {
    if (other.isSetGraphNode()) {
      this.graphNode = new RDF_Term(other.graphNode);
    }
    if (other.isSetPrefix()) {
      this.prefix = other.prefix;
    }
  }

  public Patch_Prefix_Del deepCopy() {
    return new Patch_Prefix_Del(this);
  }

  @Override
  public void clear() {
    this.graphNode = null;
    this.prefix = null;
  }

  public RDF_Term getGraphNode() {
    return this.graphNode;
  }

  public Patch_Prefix_Del setGraphNode(RDF_Term graphNode) {
    this.graphNode = graphNode;
    return this;
  }

  public void unsetGraphNode() {
    this.graphNode = null;
  }

  /** Returns true if field graphNode is set (has been assigned a value) and false otherwise */
  public boolean isSetGraphNode() {
    return this.graphNode != null;
  }

  public void setGraphNodeIsSet(boolean value) {
    if (!value) {
      this.graphNode = null;
    }
  }

  public String getPrefix() {
    return this.prefix;
  }

  public Patch_Prefix_Del setPrefix(String prefix) {
    this.prefix = prefix;
    return this;
  }

  public void unsetPrefix() {
    this.prefix = null;
  }

  /** Returns true if field prefix is set (has been assigned a value) and false otherwise */
  public boolean isSetPrefix() {
    return this.prefix != null;
  }

  public void setPrefixIsSet(boolean value) {
    if (!value) {
      this.prefix = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case GRAPH_NODE:
      if (value == null) {
        unsetGraphNode();
      } else {
        setGraphNode((RDF_Term)value);
      }
      break;

    case PREFIX:
      if (value == null) {
        unsetPrefix();
      } else {
        setPrefix((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case GRAPH_NODE:
      return getGraphNode();

    case PREFIX:
      return getPrefix();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case GRAPH_NODE:
      return isSetGraphNode();
    case PREFIX:
      return isSetPrefix();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof Patch_Prefix_Del)
      return this.equals((Patch_Prefix_Del)that);
    return false;
  }

  public boolean equals(Patch_Prefix_Del that) {
    if (that == null)
      return false;

    boolean this_present_graphNode = true && this.isSetGraphNode();
    boolean that_present_graphNode = true && that.isSetGraphNode();
    if (this_present_graphNode || that_present_graphNode) {
      if (!(this_present_graphNode && that_present_graphNode))
        return false;
      if (!this.graphNode.equals(that.graphNode))
        return false;
    }

    boolean this_present_prefix = true && this.isSetPrefix();
    boolean that_present_prefix = true && that.isSetPrefix();
    if (this_present_prefix || that_present_prefix) {
      if (!(this_present_prefix && that_present_prefix))
        return false;
      if (!this.prefix.equals(that.prefix))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(Patch_Prefix_Del other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetGraphNode()).compareTo(other.isSetGraphNode());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetGraphNode()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.graphNode, other.graphNode);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetPrefix()).compareTo(other.isSetPrefix());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetPrefix()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.prefix, other.prefix);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("Patch_Prefix_Del(");
    boolean first = true;

    if (isSetGraphNode()) {
      sb.append("graphNode:");
      if (this.graphNode == null) {
        sb.append("null");
      } else {
        sb.append(this.graphNode);
      }
      first = false;
    }
    if (!first) sb.append(", ");
    sb.append("prefix:");
    if (this.prefix == null) {
      sb.append("null");
    } else {
      sb.append(this.prefix);
    }
    first = false;
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (prefix == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'prefix' was not present! Struct: " + toString());
    }
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class Patch_Prefix_DelStandardSchemeFactory implements SchemeFactory {
    public Patch_Prefix_DelStandardScheme getScheme() {
      return new Patch_Prefix_DelStandardScheme();
    }
  }

  private static class Patch_Prefix_DelStandardScheme extends StandardScheme<Patch_Prefix_Del> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, Patch_Prefix_Del struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // GRAPH_NODE
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.graphNode = new RDF_Term();
              struct.graphNode.read(iprot);
              struct.setGraphNodeIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // PREFIX
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.prefix = iprot.readString();
              struct.setPrefixIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();

      // check for required fields of primitive type, which can't be checked in the validate method
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, Patch_Prefix_Del struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.graphNode != null) {
        if (struct.isSetGraphNode()) {
          oprot.writeFieldBegin(GRAPH_NODE_FIELD_DESC);
          struct.graphNode.write(oprot);
          oprot.writeFieldEnd();
        }
      }
      if (struct.prefix != null) {
        oprot.writeFieldBegin(PREFIX_FIELD_DESC);
        oprot.writeString(struct.prefix);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class Patch_Prefix_DelTupleSchemeFactory implements SchemeFactory {
    public Patch_Prefix_DelTupleScheme getScheme() {
      return new Patch_Prefix_DelTupleScheme();
    }
  }

  private static class Patch_Prefix_DelTupleScheme extends TupleScheme<Patch_Prefix_Del> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, Patch_Prefix_Del struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.prefix);
      BitSet optionals = new BitSet();
      if (struct.isSetGraphNode()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetGraphNode()) {
        struct.graphNode.write(oprot);
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, Patch_Prefix_Del struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.prefix = iprot.readString();
      struct.setPrefixIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        struct.graphNode = new RDF_Term();
        struct.graphNode.read(iprot);
        struct.setGraphNodeIsSet(true);
      }
    }
  }

}

