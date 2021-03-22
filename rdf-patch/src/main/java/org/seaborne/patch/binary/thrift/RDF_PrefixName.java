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

import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Collections;

@SuppressWarnings("all")
public class RDF_PrefixName implements org.apache.thrift.TBase<RDF_PrefixName, RDF_PrefixName._Fields>, java.io.Serializable, Cloneable, Comparable<RDF_PrefixName> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RDF_PrefixName");

  private static final org.apache.thrift.protocol.TField PREFIX_FIELD_DESC = new org.apache.thrift.protocol.TField("prefix", org.apache.thrift.protocol.TType.STRING, (short)1);
  private static final org.apache.thrift.protocol.TField LOCAL_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("localName", org.apache.thrift.protocol.TType.STRING, (short)2);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new RDF_PrefixNameStandardSchemeFactory());
    schemes.put(TupleScheme.class, new RDF_PrefixNameTupleSchemeFactory());
  }

  public String prefix; // required
  public String localName; // required

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    PREFIX((short)1, "prefix"),
    LOCAL_NAME((short)2, "localName");

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
        case 1: // PREFIX
          return PREFIX;
        case 2: // LOCAL_NAME
          return LOCAL_NAME;
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
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.PREFIX, new org.apache.thrift.meta_data.FieldMetaData("prefix", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    tmpMap.put(_Fields.LOCAL_NAME, new org.apache.thrift.meta_data.FieldMetaData("localName", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.STRING)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RDF_PrefixName.class, metaDataMap);
  }

  public RDF_PrefixName() {
  }

  public RDF_PrefixName(
    String prefix,
    String localName)
  {
    this();
    this.prefix = prefix;
    this.localName = localName;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public RDF_PrefixName(RDF_PrefixName other) {
    if (other.isSetPrefix()) {
      this.prefix = other.prefix;
    }
    if (other.isSetLocalName()) {
      this.localName = other.localName;
    }
  }

  public RDF_PrefixName deepCopy() {
    return new RDF_PrefixName(this);
  }

  @Override
  public void clear() {
    this.prefix = null;
    this.localName = null;
  }

  public String getPrefix() {
    return this.prefix;
  }

  public RDF_PrefixName setPrefix(String prefix) {
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

  public String getLocalName() {
    return this.localName;
  }

  public RDF_PrefixName setLocalName(String localName) {
    this.localName = localName;
    return this;
  }

  public void unsetLocalName() {
    this.localName = null;
  }

  /** Returns true if field localName is set (has been assigned a value) and false otherwise */
  public boolean isSetLocalName() {
    return this.localName != null;
  }

  public void setLocalNameIsSet(boolean value) {
    if (!value) {
      this.localName = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case PREFIX:
      if (value == null) {
        unsetPrefix();
      } else {
        setPrefix((String)value);
      }
      break;

    case LOCAL_NAME:
      if (value == null) {
        unsetLocalName();
      } else {
        setLocalName((String)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case PREFIX:
      return getPrefix();

    case LOCAL_NAME:
      return getLocalName();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case PREFIX:
      return isSetPrefix();
    case LOCAL_NAME:
      return isSetLocalName();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof RDF_PrefixName)
      return this.equals((RDF_PrefixName)that);
    return false;
  }

  public boolean equals(RDF_PrefixName that) {
    if (that == null)
      return false;

    boolean this_present_prefix = true && this.isSetPrefix();
    boolean that_present_prefix = true && that.isSetPrefix();
    if (this_present_prefix || that_present_prefix) {
      if (!(this_present_prefix && that_present_prefix))
        return false;
      if (!this.prefix.equals(that.prefix))
        return false;
    }

    boolean this_present_localName = true && this.isSetLocalName();
    boolean that_present_localName = true && that.isSetLocalName();
    if (this_present_localName || that_present_localName) {
      if (!(this_present_localName && that_present_localName))
        return false;
      if (!this.localName.equals(that.localName))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(RDF_PrefixName other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

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
    lastComparison = Boolean.valueOf(isSetLocalName()).compareTo(other.isSetLocalName());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetLocalName()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.localName, other.localName);
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
    StringBuilder sb = new StringBuilder("RDF_PrefixName(");
    boolean first = true;

    sb.append("prefix:");
    if (this.prefix == null) {
      sb.append("null");
    } else {
      sb.append(this.prefix);
    }
    first = false;
    if (!first) sb.append(", ");
    sb.append("localName:");
    if (this.localName == null) {
      sb.append("null");
    } else {
      sb.append(this.localName);
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
    if (localName == null) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'localName' was not present! Struct: " + toString());
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

  private static class RDF_PrefixNameStandardSchemeFactory implements SchemeFactory {
    public RDF_PrefixNameStandardScheme getScheme() {
      return new RDF_PrefixNameStandardScheme();
    }
  }

  private static class RDF_PrefixNameStandardScheme extends StandardScheme<RDF_PrefixName> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, RDF_PrefixName struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // PREFIX
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.prefix = iprot.readString();
              struct.setPrefixIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // LOCAL_NAME
            if (schemeField.type == org.apache.thrift.protocol.TType.STRING) {
              struct.localName = iprot.readString();
              struct.setLocalNameIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, RDF_PrefixName struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.prefix != null) {
        oprot.writeFieldBegin(PREFIX_FIELD_DESC);
        oprot.writeString(struct.prefix);
        oprot.writeFieldEnd();
      }
      if (struct.localName != null) {
        oprot.writeFieldBegin(LOCAL_NAME_FIELD_DESC);
        oprot.writeString(struct.localName);
        oprot.writeFieldEnd();
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class RDF_PrefixNameTupleSchemeFactory implements SchemeFactory {
    public RDF_PrefixNameTupleScheme getScheme() {
      return new RDF_PrefixNameTupleScheme();
    }
  }

  private static class RDF_PrefixNameTupleScheme extends TupleScheme<RDF_PrefixName> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, RDF_PrefixName struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeString(struct.prefix);
      oprot.writeString(struct.localName);
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, RDF_PrefixName struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.prefix = iprot.readString();
      struct.setPrefixIsSet(true);
      struct.localName = iprot.readString();
      struct.setLocalNameIsSet(true);
    }
  }

}

