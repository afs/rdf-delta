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
public class RDF_Term extends org.apache.thrift.TUnion<RDF_Term, RDF_Term._Fields> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("RDF_Term");
  private static final org.apache.thrift.protocol.TField IRI_FIELD_DESC = new org.apache.thrift.protocol.TField("iri", org.apache.thrift.protocol.TType.STRUCT, (short)1);
  private static final org.apache.thrift.protocol.TField BNODE_FIELD_DESC = new org.apache.thrift.protocol.TField("bnode", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField LITERAL_FIELD_DESC = new org.apache.thrift.protocol.TField("literal", org.apache.thrift.protocol.TType.STRUCT, (short)3);
  private static final org.apache.thrift.protocol.TField PREFIX_NAME_FIELD_DESC = new org.apache.thrift.protocol.TField("prefixName", org.apache.thrift.protocol.TType.STRUCT, (short)4);
  private static final org.apache.thrift.protocol.TField VARIABLE_FIELD_DESC = new org.apache.thrift.protocol.TField("variable", org.apache.thrift.protocol.TType.STRUCT, (short)5);
  private static final org.apache.thrift.protocol.TField ANY_FIELD_DESC = new org.apache.thrift.protocol.TField("any", org.apache.thrift.protocol.TType.STRUCT, (short)6);
  private static final org.apache.thrift.protocol.TField UNDEFINED_FIELD_DESC = new org.apache.thrift.protocol.TField("undefined", org.apache.thrift.protocol.TType.STRUCT, (short)7);
  private static final org.apache.thrift.protocol.TField REPEAT_FIELD_DESC = new org.apache.thrift.protocol.TField("repeat", org.apache.thrift.protocol.TType.STRUCT, (short)8);
  private static final org.apache.thrift.protocol.TField VAL_INTEGER_FIELD_DESC = new org.apache.thrift.protocol.TField("valInteger", org.apache.thrift.protocol.TType.I64, (short)10);
  private static final org.apache.thrift.protocol.TField VAL_DOUBLE_FIELD_DESC = new org.apache.thrift.protocol.TField("valDouble", org.apache.thrift.protocol.TType.DOUBLE, (short)11);
  private static final org.apache.thrift.protocol.TField VAL_DECIMAL_FIELD_DESC = new org.apache.thrift.protocol.TField("valDecimal", org.apache.thrift.protocol.TType.STRUCT, (short)12);

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    IRI((short)1, "iri"),
    BNODE((short)2, "bnode"),
    LITERAL((short)3, "literal"),
    PREFIX_NAME((short)4, "prefixName"),
    VARIABLE((short)5, "variable"),
    ANY((short)6, "any"),
    UNDEFINED((short)7, "undefined"),
    REPEAT((short)8, "repeat"),
    VAL_INTEGER((short)10, "valInteger"),
    VAL_DOUBLE((short)11, "valDouble"),
    VAL_DECIMAL((short)12, "valDecimal");

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
        case 1: // IRI
          return IRI;
        case 2: // BNODE
          return BNODE;
        case 3: // LITERAL
          return LITERAL;
        case 4: // PREFIX_NAME
          return PREFIX_NAME;
        case 5: // VARIABLE
          return VARIABLE;
        case 6: // ANY
          return ANY;
        case 7: // UNDEFINED
          return UNDEFINED;
        case 8: // REPEAT
          return REPEAT;
        case 10: // VAL_INTEGER
          return VAL_INTEGER;
        case 11: // VAL_DOUBLE
          return VAL_DOUBLE;
        case 12: // VAL_DECIMAL
          return VAL_DECIMAL;
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

  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.IRI, new org.apache.thrift.meta_data.FieldMetaData("iri", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_IRI.class)));
    tmpMap.put(_Fields.BNODE, new org.apache.thrift.meta_data.FieldMetaData("bnode", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_BNode.class)));
    tmpMap.put(_Fields.LITERAL, new org.apache.thrift.meta_data.FieldMetaData("literal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_Literal.class)));
    tmpMap.put(_Fields.PREFIX_NAME, new org.apache.thrift.meta_data.FieldMetaData("prefixName", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_PrefixName.class)));
    tmpMap.put(_Fields.VARIABLE, new org.apache.thrift.meta_data.FieldMetaData("variable", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_VAR.class)));
    tmpMap.put(_Fields.ANY, new org.apache.thrift.meta_data.FieldMetaData("any", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_ANY.class)));
    tmpMap.put(_Fields.UNDEFINED, new org.apache.thrift.meta_data.FieldMetaData("undefined", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_UNDEF.class)));
    tmpMap.put(_Fields.REPEAT, new org.apache.thrift.meta_data.FieldMetaData("repeat", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_REPEAT.class)));
    tmpMap.put(_Fields.VAL_INTEGER, new org.apache.thrift.meta_data.FieldMetaData("valInteger", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.VAL_DOUBLE, new org.apache.thrift.meta_data.FieldMetaData("valDouble", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.DOUBLE)));
    tmpMap.put(_Fields.VAL_DECIMAL, new org.apache.thrift.meta_data.FieldMetaData("valDecimal", org.apache.thrift.TFieldRequirementType.DEFAULT, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, RDF_Decimal.class)));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(RDF_Term.class, metaDataMap);
  }

  public RDF_Term() {
    super();
  }

  public RDF_Term(_Fields setField, Object value) {
    super(setField, value);
  }

  public RDF_Term(RDF_Term other) {
    super(other);
  }
  public RDF_Term deepCopy() {
    return new RDF_Term(this);
  }

  public static RDF_Term iri(RDF_IRI value) {
    RDF_Term x = new RDF_Term();
    x.setIri(value);
    return x;
  }

  public static RDF_Term bnode(RDF_BNode value) {
    RDF_Term x = new RDF_Term();
    x.setBnode(value);
    return x;
  }

  public static RDF_Term literal(RDF_Literal value) {
    RDF_Term x = new RDF_Term();
    x.setLiteral(value);
    return x;
  }

  public static RDF_Term prefixName(RDF_PrefixName value) {
    RDF_Term x = new RDF_Term();
    x.setPrefixName(value);
    return x;
  }

  public static RDF_Term variable(RDF_VAR value) {
    RDF_Term x = new RDF_Term();
    x.setVariable(value);
    return x;
  }

  public static RDF_Term any(RDF_ANY value) {
    RDF_Term x = new RDF_Term();
    x.setAny(value);
    return x;
  }

  public static RDF_Term undefined(RDF_UNDEF value) {
    RDF_Term x = new RDF_Term();
    x.setUndefined(value);
    return x;
  }

  public static RDF_Term repeat(RDF_REPEAT value) {
    RDF_Term x = new RDF_Term();
    x.setRepeat(value);
    return x;
  }

  public static RDF_Term valInteger(long value) {
    RDF_Term x = new RDF_Term();
    x.setValInteger(value);
    return x;
  }

  public static RDF_Term valDouble(double value) {
    RDF_Term x = new RDF_Term();
    x.setValDouble(value);
    return x;
  }

  public static RDF_Term valDecimal(RDF_Decimal value) {
    RDF_Term x = new RDF_Term();
    x.setValDecimal(value);
    return x;
  }


  @Override
  protected void checkType(_Fields setField, Object value) throws ClassCastException {
    switch (setField) {
      case IRI:
        if (value instanceof RDF_IRI) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_IRI for field 'iri', but got " + value.getClass().getSimpleName());
      case BNODE:
        if (value instanceof RDF_BNode) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_BNode for field 'bnode', but got " + value.getClass().getSimpleName());
      case LITERAL:
        if (value instanceof RDF_Literal) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_Literal for field 'literal', but got " + value.getClass().getSimpleName());
      case PREFIX_NAME:
        if (value instanceof RDF_PrefixName) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_PrefixName for field 'prefixName', but got " + value.getClass().getSimpleName());
      case VARIABLE:
        if (value instanceof RDF_VAR) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_VAR for field 'variable', but got " + value.getClass().getSimpleName());
      case ANY:
        if (value instanceof RDF_ANY) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_ANY for field 'any', but got " + value.getClass().getSimpleName());
      case UNDEFINED:
        if (value instanceof RDF_UNDEF) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_UNDEF for field 'undefined', but got " + value.getClass().getSimpleName());
      case REPEAT:
        if (value instanceof RDF_REPEAT) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_REPEAT for field 'repeat', but got " + value.getClass().getSimpleName());
      case VAL_INTEGER:
        if (value instanceof Long) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Long for field 'valInteger', but got " + value.getClass().getSimpleName());
      case VAL_DOUBLE:
        if (value instanceof Double) {
          break;
        }
        throw new ClassCastException("Was expecting value of type Double for field 'valDouble', but got " + value.getClass().getSimpleName());
      case VAL_DECIMAL:
        if (value instanceof RDF_Decimal) {
          break;
        }
        throw new ClassCastException("Was expecting value of type RDF_Decimal for field 'valDecimal', but got " + value.getClass().getSimpleName());
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected Object standardSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, org.apache.thrift.protocol.TField field) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(field.id);
    if (setField != null) {
      switch (setField) {
        case IRI:
          if (field.type == IRI_FIELD_DESC.type) {
            RDF_IRI iri;
            iri = new RDF_IRI();
            iri.read(iprot);
            return iri;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case BNODE:
          if (field.type == BNODE_FIELD_DESC.type) {
            RDF_BNode bnode;
            bnode = new RDF_BNode();
            bnode.read(iprot);
            return bnode;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case LITERAL:
          if (field.type == LITERAL_FIELD_DESC.type) {
            RDF_Literal literal;
            literal = new RDF_Literal();
            literal.read(iprot);
            return literal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case PREFIX_NAME:
          if (field.type == PREFIX_NAME_FIELD_DESC.type) {
            RDF_PrefixName prefixName;
            prefixName = new RDF_PrefixName();
            prefixName.read(iprot);
            return prefixName;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case VARIABLE:
          if (field.type == VARIABLE_FIELD_DESC.type) {
            RDF_VAR variable;
            variable = new RDF_VAR();
            variable.read(iprot);
            return variable;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case ANY:
          if (field.type == ANY_FIELD_DESC.type) {
            RDF_ANY any;
            any = new RDF_ANY();
            any.read(iprot);
            return any;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case UNDEFINED:
          if (field.type == UNDEFINED_FIELD_DESC.type) {
            RDF_UNDEF undefined;
            undefined = new RDF_UNDEF();
            undefined.read(iprot);
            return undefined;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case REPEAT:
          if (field.type == REPEAT_FIELD_DESC.type) {
            RDF_REPEAT repeat;
            repeat = new RDF_REPEAT();
            repeat.read(iprot);
            return repeat;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case VAL_INTEGER:
          if (field.type == VAL_INTEGER_FIELD_DESC.type) {
            Long valInteger;
            valInteger = iprot.readI64();
            return valInteger;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case VAL_DOUBLE:
          if (field.type == VAL_DOUBLE_FIELD_DESC.type) {
            Double valDouble;
            valDouble = iprot.readDouble();
            return valDouble;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        case VAL_DECIMAL:
          if (field.type == VAL_DECIMAL_FIELD_DESC.type) {
            RDF_Decimal valDecimal;
            valDecimal = new RDF_Decimal();
            valDecimal.read(iprot);
            return valDecimal;
          } else {
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
            return null;
          }
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      org.apache.thrift.protocol.TProtocolUtil.skip(iprot, field.type);
      return null;
    }
  }

  @Override
  protected void standardSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case IRI:
        RDF_IRI iri = (RDF_IRI)value_;
        iri.write(oprot);
        return;
      case BNODE:
        RDF_BNode bnode = (RDF_BNode)value_;
        bnode.write(oprot);
        return;
      case LITERAL:
        RDF_Literal literal = (RDF_Literal)value_;
        literal.write(oprot);
        return;
      case PREFIX_NAME:
        RDF_PrefixName prefixName = (RDF_PrefixName)value_;
        prefixName.write(oprot);
        return;
      case VARIABLE:
        RDF_VAR variable = (RDF_VAR)value_;
        variable.write(oprot);
        return;
      case ANY:
        RDF_ANY any = (RDF_ANY)value_;
        any.write(oprot);
        return;
      case UNDEFINED:
        RDF_UNDEF undefined = (RDF_UNDEF)value_;
        undefined.write(oprot);
        return;
      case REPEAT:
        RDF_REPEAT repeat = (RDF_REPEAT)value_;
        repeat.write(oprot);
        return;
      case VAL_INTEGER:
        Long valInteger = (Long)value_;
        oprot.writeI64(valInteger);
        return;
      case VAL_DOUBLE:
        Double valDouble = (Double)value_;
        oprot.writeDouble(valDouble);
        return;
      case VAL_DECIMAL:
        RDF_Decimal valDecimal = (RDF_Decimal)value_;
        valDecimal.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected Object tupleSchemeReadValue(org.apache.thrift.protocol.TProtocol iprot, short fieldID) throws org.apache.thrift.TException {
    _Fields setField = _Fields.findByThriftId(fieldID);
    if (setField != null) {
      switch (setField) {
        case IRI:
          RDF_IRI iri;
          iri = new RDF_IRI();
          iri.read(iprot);
          return iri;
        case BNODE:
          RDF_BNode bnode;
          bnode = new RDF_BNode();
          bnode.read(iprot);
          return bnode;
        case LITERAL:
          RDF_Literal literal;
          literal = new RDF_Literal();
          literal.read(iprot);
          return literal;
        case PREFIX_NAME:
          RDF_PrefixName prefixName;
          prefixName = new RDF_PrefixName();
          prefixName.read(iprot);
          return prefixName;
        case VARIABLE:
          RDF_VAR variable;
          variable = new RDF_VAR();
          variable.read(iprot);
          return variable;
        case ANY:
          RDF_ANY any;
          any = new RDF_ANY();
          any.read(iprot);
          return any;
        case UNDEFINED:
          RDF_UNDEF undefined;
          undefined = new RDF_UNDEF();
          undefined.read(iprot);
          return undefined;
        case REPEAT:
          RDF_REPEAT repeat;
          repeat = new RDF_REPEAT();
          repeat.read(iprot);
          return repeat;
        case VAL_INTEGER:
          Long valInteger;
          valInteger = iprot.readI64();
          return valInteger;
        case VAL_DOUBLE:
          Double valDouble;
          valDouble = iprot.readDouble();
          return valDouble;
        case VAL_DECIMAL:
          RDF_Decimal valDecimal;
          valDecimal = new RDF_Decimal();
          valDecimal.read(iprot);
          return valDecimal;
        default:
          throw new IllegalStateException("setField wasn't null, but didn't match any of the case statements!");
      }
    } else {
      throw new TProtocolException("Couldn't find a field with field id " + fieldID);
    }
  }

  @Override
  protected void tupleSchemeWriteValue(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    switch (setField_) {
      case IRI:
        RDF_IRI iri = (RDF_IRI)value_;
        iri.write(oprot);
        return;
      case BNODE:
        RDF_BNode bnode = (RDF_BNode)value_;
        bnode.write(oprot);
        return;
      case LITERAL:
        RDF_Literal literal = (RDF_Literal)value_;
        literal.write(oprot);
        return;
      case PREFIX_NAME:
        RDF_PrefixName prefixName = (RDF_PrefixName)value_;
        prefixName.write(oprot);
        return;
      case VARIABLE:
        RDF_VAR variable = (RDF_VAR)value_;
        variable.write(oprot);
        return;
      case ANY:
        RDF_ANY any = (RDF_ANY)value_;
        any.write(oprot);
        return;
      case UNDEFINED:
        RDF_UNDEF undefined = (RDF_UNDEF)value_;
        undefined.write(oprot);
        return;
      case REPEAT:
        RDF_REPEAT repeat = (RDF_REPEAT)value_;
        repeat.write(oprot);
        return;
      case VAL_INTEGER:
        Long valInteger = (Long)value_;
        oprot.writeI64(valInteger);
        return;
      case VAL_DOUBLE:
        Double valDouble = (Double)value_;
        oprot.writeDouble(valDouble);
        return;
      case VAL_DECIMAL:
        RDF_Decimal valDecimal = (RDF_Decimal)value_;
        valDecimal.write(oprot);
        return;
      default:
        throw new IllegalStateException("Cannot write union with unknown field " + setField_);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TField getFieldDesc(_Fields setField) {
    switch (setField) {
      case IRI:
        return IRI_FIELD_DESC;
      case BNODE:
        return BNODE_FIELD_DESC;
      case LITERAL:
        return LITERAL_FIELD_DESC;
      case PREFIX_NAME:
        return PREFIX_NAME_FIELD_DESC;
      case VARIABLE:
        return VARIABLE_FIELD_DESC;
      case ANY:
        return ANY_FIELD_DESC;
      case UNDEFINED:
        return UNDEFINED_FIELD_DESC;
      case REPEAT:
        return REPEAT_FIELD_DESC;
      case VAL_INTEGER:
        return VAL_INTEGER_FIELD_DESC;
      case VAL_DOUBLE:
        return VAL_DOUBLE_FIELD_DESC;
      case VAL_DECIMAL:
        return VAL_DECIMAL_FIELD_DESC;
      default:
        throw new IllegalArgumentException("Unknown field id " + setField);
    }
  }

  @Override
  protected org.apache.thrift.protocol.TStruct getStructDesc() {
    return STRUCT_DESC;
  }

  @Override
  protected _Fields enumForId(short id) {
    return _Fields.findByThriftIdOrThrow(id);
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }


  public RDF_IRI getIri() {
    if (getSetField() == _Fields.IRI) {
      return (RDF_IRI)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'iri' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setIri(RDF_IRI value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.IRI;
    value_ = value;
  }

  public RDF_BNode getBnode() {
    if (getSetField() == _Fields.BNODE) {
      return (RDF_BNode)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'bnode' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setBnode(RDF_BNode value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.BNODE;
    value_ = value;
  }

  public RDF_Literal getLiteral() {
    if (getSetField() == _Fields.LITERAL) {
      return (RDF_Literal)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'literal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setLiteral(RDF_Literal value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.LITERAL;
    value_ = value;
  }

  public RDF_PrefixName getPrefixName() {
    if (getSetField() == _Fields.PREFIX_NAME) {
      return (RDF_PrefixName)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'prefixName' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setPrefixName(RDF_PrefixName value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.PREFIX_NAME;
    value_ = value;
  }

  public RDF_VAR getVariable() {
    if (getSetField() == _Fields.VARIABLE) {
      return (RDF_VAR)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'variable' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setVariable(RDF_VAR value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.VARIABLE;
    value_ = value;
  }

  public RDF_ANY getAny() {
    if (getSetField() == _Fields.ANY) {
      return (RDF_ANY)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'any' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setAny(RDF_ANY value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.ANY;
    value_ = value;
  }

  public RDF_UNDEF getUndefined() {
    if (getSetField() == _Fields.UNDEFINED) {
      return (RDF_UNDEF)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'undefined' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setUndefined(RDF_UNDEF value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.UNDEFINED;
    value_ = value;
  }

  public RDF_REPEAT getRepeat() {
    if (getSetField() == _Fields.REPEAT) {
      return (RDF_REPEAT)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'repeat' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setRepeat(RDF_REPEAT value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.REPEAT;
    value_ = value;
  }

  public long getValInteger() {
    if (getSetField() == _Fields.VAL_INTEGER) {
      return (Long)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'valInteger' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setValInteger(long value) {
    setField_ = _Fields.VAL_INTEGER;
    value_ = value;
  }

  public double getValDouble() {
    if (getSetField() == _Fields.VAL_DOUBLE) {
      return (Double)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'valDouble' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setValDouble(double value) {
    setField_ = _Fields.VAL_DOUBLE;
    value_ = value;
  }

  public RDF_Decimal getValDecimal() {
    if (getSetField() == _Fields.VAL_DECIMAL) {
      return (RDF_Decimal)getFieldValue();
    } else {
      throw new RuntimeException("Cannot get field 'valDecimal' because union is currently set to " + getFieldDesc(getSetField()).name);
    }
  }

  public void setValDecimal(RDF_Decimal value) {
    if (value == null) throw new NullPointerException();
    setField_ = _Fields.VAL_DECIMAL;
    value_ = value;
  }

  public boolean isSetIri() {
    return setField_ == _Fields.IRI;
  }


  public boolean isSetBnode() {
    return setField_ == _Fields.BNODE;
  }


  public boolean isSetLiteral() {
    return setField_ == _Fields.LITERAL;
  }


  public boolean isSetPrefixName() {
    return setField_ == _Fields.PREFIX_NAME;
  }


  public boolean isSetVariable() {
    return setField_ == _Fields.VARIABLE;
  }


  public boolean isSetAny() {
    return setField_ == _Fields.ANY;
  }


  public boolean isSetUndefined() {
    return setField_ == _Fields.UNDEFINED;
  }


  public boolean isSetRepeat() {
    return setField_ == _Fields.REPEAT;
  }


  public boolean isSetValInteger() {
    return setField_ == _Fields.VAL_INTEGER;
  }


  public boolean isSetValDouble() {
    return setField_ == _Fields.VAL_DOUBLE;
  }


  public boolean isSetValDecimal() {
    return setField_ == _Fields.VAL_DECIMAL;
  }


  public boolean equals(Object other) {
    if (other instanceof RDF_Term) {
      return equals((RDF_Term)other);
    } else {
      return false;
    }
  }

  public boolean equals(RDF_Term other) {
    return other != null && getSetField() == other.getSetField() && getFieldValue().equals(other.getFieldValue());
  }

  @Override
  public int compareTo(RDF_Term other) {
    int lastComparison = org.apache.thrift.TBaseHelper.compareTo(getSetField(), other.getSetField());
    if (lastComparison == 0) {
      return org.apache.thrift.TBaseHelper.compareTo(getFieldValue(), other.getFieldValue());
    }
    return lastComparison;
  }


  /**
   * If you'd like this to perform more respectably, use the hashcode generator option.
   */
  @Override
  public int hashCode() {
    return 0;
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


}