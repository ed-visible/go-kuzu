package ryu

// #include "ryu.h"
// #include <stdlib.h>
// #include <string.h>
import "C"

import (
	"fmt"
	"reflect"
	"sort"
	"time"
	"unsafe"

	"math/big"

	"github.com/google/uuid"
	"github.com/shopspring/decimal"
)

// InternalID represents the internal ID of a node or relationship in Ryu.
type InternalID struct {
	TableID uint64
	Offset  uint64
}

// Node represents a node retrieved from Ryu.
// A node has an ID, a label, and properties.
type Node struct {
	ID         InternalID
	Label      string
	Properties map[string]any
}

// Relationship represents a relationship retrieved from Ryu.
// A relationship has a source ID, a destination ID, a label, and properties.
type Relationship struct {
	ID            InternalID
	SourceID      InternalID
	DestinationID InternalID
	Label         string
	Properties    map[string]any
}

// RecursiveRelationship represents a recursive relationship retrieved from a
// path query in Ryu. A recursive relationship has a list of nodes and a list
// of relationships.
type RecursiveRelationship struct {
	Nodes         []Node
	Relationships []Relationship
}

// MapItem represents a key-value pair in a map in Ryu. It is used for both
// the query parameters and the query result.
type MapItem struct {
	Key   any
	Value any
}

// ryuNodeValueToGoValue converts a ryu_value representing a node to a Node
// struct in Go.
func ryuNodeValueToGoValue(ryuValue C.ryu_value) (Node, error) {
	node := Node{}
	node.Properties = make(map[string]any)
	idValue := C.ryu_value{}
	C.ryu_node_val_get_id_val(&ryuValue, &idValue)
	nodeId, _ := ryuValueToGoValue(idValue)
	node.ID = nodeId.(InternalID)
	C.ryu_value_destroy(&idValue)
	labelValue := C.ryu_value{}
	C.ryu_node_val_get_label_val(&ryuValue, &labelValue)
	nodeLabel, _ := ryuValueToGoValue(labelValue)
	node.Label = nodeLabel.(string)
	C.ryu_value_destroy(&labelValue)
	var propertySize C.uint64_t
	C.ryu_node_val_get_property_size(&ryuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.ryu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.ryu_node_val_get_property_name_at(&ryuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.ryu_destroy_string(currentKey)
		C.ryu_node_val_get_property_value_at(&ryuValue, i, &currentVal)
		value, err := ryuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		node.Properties[keyString] = value
		C.ryu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return node, fmt.Errorf("failed to get values: %v", errors)
	}
	return node, nil
}

// ryuRelValueToGoValue converts a ryu_value representing a relationship to a
// Relationship struct in Go.
func ryuRelValueToGoValue(ryuValue C.ryu_value) (Relationship, error) {
	relation := Relationship{}
	relation.Properties = make(map[string]any)
	idValue := C.ryu_value{}
	C.ryu_rel_val_get_id_val(&ryuValue, &idValue)
	id, _ := ryuValueToGoValue(idValue)
	relation.ID = id.(InternalID)
	C.ryu_value_destroy(&idValue)
	C.ryu_rel_val_get_src_id_val(&ryuValue, &idValue)
	src, _ := ryuValueToGoValue(idValue)
	relation.SourceID = src.(InternalID)
	C.ryu_value_destroy(&idValue)
	C.ryu_rel_val_get_dst_id_val(&ryuValue, &idValue)
	dst, _ := ryuValueToGoValue(idValue)
	relation.DestinationID = dst.(InternalID)
	C.ryu_value_destroy(&idValue)
	labelValue := C.ryu_value{}
	C.ryu_rel_val_get_label_val(&ryuValue, &labelValue)
	label, _ := ryuValueToGoValue(labelValue)
	relation.Label = label.(string)
	C.ryu_value_destroy(&labelValue)
	var propertySize C.uint64_t
	C.ryu_rel_val_get_property_size(&ryuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.ryu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.ryu_rel_val_get_property_name_at(&ryuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.ryu_destroy_string(currentKey)
		C.ryu_rel_val_get_property_value_at(&ryuValue, i, &currentVal)
		value, err := ryuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		relation.Properties[keyString] = value
		C.ryu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return relation, fmt.Errorf("failed to get values: %v", errors)
	}
	return relation, nil
}

// ryuRecursiveRelValueToGoValue converts a ryu_value representing a recursive
// relationship to a RecursiveRelationship struct in Go.
func ryuRecursiveRelValueToGoValue(ryuValue C.ryu_value) (RecursiveRelationship, error) {
	var nodesVal C.ryu_value
	var relsVal C.ryu_value
	C.ryu_value_get_recursive_rel_node_list(&ryuValue, &nodesVal)
	C.ryu_value_get_recursive_rel_rel_list(&ryuValue, &relsVal)
	defer C.ryu_value_destroy(&nodesVal)
	defer C.ryu_value_destroy(&relsVal)
	nodes, _ := ryuListValueToGoValue(nodesVal)
	rels, _ := ryuListValueToGoValue(relsVal)
	recursiveRel := RecursiveRelationship{}
	recursiveRel.Nodes = make([]Node, len(nodes))
	for i, n := range nodes {
		recursiveRel.Nodes[i] = n.(Node)
	}
	relationships := make([]Relationship, len(rels))
	for i, r := range rels {
		relationships[i] = r.(Relationship)
	}
	recursiveRel.Relationships = relationships
	return recursiveRel, nil
}

// ryuListValueToGoValue converts a ryu_value representing a LIST or ARRAY to
// a slice of any in Go.
func ryuListValueToGoValue(ryuValue C.ryu_value) ([]any, error) {
	var listSize C.uint64_t
	cLogicalType := C.ryu_logical_type{}
	defer C.ryu_data_type_destroy(&cLogicalType)
	C.ryu_value_get_data_type(&ryuValue, &cLogicalType)
	logicalTypeId := C.ryu_data_type_get_id(&cLogicalType)
	if logicalTypeId == C.RYU_ARRAY {
		C.ryu_data_type_get_num_elements_in_array(&cLogicalType, &listSize)
	} else {
		C.ryu_value_get_list_size(&ryuValue, &listSize)
	}
	list := make([]any, 0, int(listSize))
	var currentVal C.ryu_value
	var errors []error
	for i := C.uint64_t(0); i < listSize; i++ {
		C.ryu_value_get_list_element(&ryuValue, i, &currentVal)
		value, err := ryuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		list = append(list, value)
		C.ryu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return list, fmt.Errorf("failed to get values: %v", errors)
	}
	return list, nil
}

// ryuStructValueToGoValue converts a ryu_value representing a STRUCT to a
// map of string to any in Go.
func ryuStructValueToGoValue(ryuValue C.ryu_value) (map[string]any, error) {
	structure := make(map[string]any)
	var propertySize C.uint64_t
	C.ryu_value_get_struct_num_fields(&ryuValue, &propertySize)
	var currentKey *C.char
	var currentVal C.ryu_value
	var errors []error
	for i := C.uint64_t(0); i < propertySize; i++ {
		C.ryu_value_get_struct_field_name(&ryuValue, i, &currentKey)
		keyString := C.GoString(currentKey)
		C.ryu_destroy_string(currentKey)
		C.ryu_value_get_struct_field_value(&ryuValue, i, &currentVal)
		value, err := ryuValueToGoValue(currentVal)
		if err != nil {
			errors = append(errors, err)
		}
		structure[keyString] = value
		C.ryu_value_destroy(&currentVal)
	}
	if len(errors) > 0 {
		return structure, fmt.Errorf("failed to get values: %v", errors)
	}
	return structure, nil
}

// ryuMapValueToGoValue converts a ryu_value representing a MAP to a
// slice of MapItem in Go.
func ryuMapValueToGoValue(ryuValue C.ryu_value) ([]MapItem, error) {
	var mapSize C.uint64_t
	C.ryu_value_get_map_size(&ryuValue, &mapSize)
	mapItems := make([]MapItem, 0, int(mapSize))
	var currentKey C.ryu_value
	var currentValue C.ryu_value
	var errors []error
	for i := C.uint64_t(0); i < mapSize; i++ {
		C.ryu_value_get_map_key(&ryuValue, i, &currentKey)
		C.ryu_value_get_map_value(&ryuValue, i, &currentValue)
		key, err := ryuValueToGoValue(currentKey)
		if err != nil {
			errors = append(errors, err)
		}
		value, err := ryuValueToGoValue(currentValue)
		if err != nil {
			errors = append(errors, err)
		}
		C.ryu_value_destroy(&currentKey)
		C.ryu_value_destroy(&currentValue)
		mapItems = append(mapItems, MapItem{Key: key, Value: value})
	}
	if len(errors) > 0 {
		return mapItems, fmt.Errorf("failed to get values: %v", errors)
	}
	return mapItems, nil
}

// ryuValueToGoValue converts a ryu_value to a corresponding Go value.
func ryuValueToGoValue(ryuValue C.ryu_value) (any, error) {
	if C.ryu_value_is_null(&ryuValue) {
		return nil, nil
	}
	var logicalType C.ryu_logical_type
	defer C.ryu_data_type_destroy(&logicalType)
	C.ryu_value_get_data_type(&ryuValue, &logicalType)
	logicalTypeId := C.ryu_data_type_get_id(&logicalType)
	switch logicalTypeId {
	case C.RYU_BOOL:
		var value C.bool
		status := C.ryu_value_get_bool(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get bool value with status: %d", status)
		}
		return bool(value), nil
	case C.RYU_INT64, C.RYU_SERIAL:
		var value C.int64_t
		status := C.ryu_value_get_int64(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get int64 value with status: %d", status)
		}
		return int64(value), nil
	case C.RYU_INT32:
		var value C.int32_t
		status := C.ryu_value_get_int32(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get int32 value with status: %d", status)
		}
		return int32(value), nil
	case C.RYU_INT16:
		var value C.int16_t
		status := C.ryu_value_get_int16(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get int16 value with status: %d", status)
		}
		return int16(value), nil
	case C.RYU_INT128:
		var value C.ryu_int128_t
		status := C.ryu_value_get_int128(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get int128 value with status: %d", status)
		}
		return int128ToBigInt(value)
	case C.RYU_INT8:
		var value C.int8_t
		status := C.ryu_value_get_int8(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get int8 value with status: %d", status)
		}
		return int8(value), nil
	case C.RYU_UUID:
		var value *C.char
		status := C.ryu_value_get_uuid(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get uuid value with status: %d", status)
		}
		defer C.ryu_destroy_string(value)
		uuidString := C.GoString(value)
		return uuid.Parse(uuidString)
	case C.RYU_UINT64:
		var value C.uint64_t
		status := C.ryu_value_get_uint64(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get uint64 value with status: %d", status)
		}
		return uint64(value), nil
	case C.RYU_UINT32:
		var value C.uint32_t
		status := C.ryu_value_get_uint32(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get uint32 value with status: %d", status)
		}
		return uint32(value), nil
	case C.RYU_UINT16:
		var value C.uint16_t
		status := C.ryu_value_get_uint16(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get uint16 value with status: %d", status)
		}
		return uint16(value), nil
	case C.RYU_UINT8:
		var value C.uint8_t
		status := C.ryu_value_get_uint8(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get uint8 value with status: %d", status)
		}
		return uint8(value), nil
	case C.RYU_DOUBLE:
		var value C.double
		status := C.ryu_value_get_double(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get double value with status: %d", status)
		}
		return float64(value), nil
	case C.RYU_FLOAT:
		var value C.float
		status := C.ryu_value_get_float(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get float value with status: %d", status)
		}
		return float32(value), nil
	case C.RYU_STRING:
		var outString *C.char
		status := C.ryu_value_get_string(&ryuValue, &outString)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get string value with status: %d", status)
		}
		defer C.ryu_destroy_string(outString)
		return C.GoString(outString), nil
	case C.RYU_TIMESTAMP:
		var value C.ryu_timestamp_t
		status := C.ryu_value_get_timestamp(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get timestamp value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000), nil
	case C.RYU_TIMESTAMP_NS:
		var value C.ryu_timestamp_ns_t
		status := C.ryu_value_get_timestamp_ns(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_ns value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)), nil
	case C.RYU_TIMESTAMP_MS:
		var value C.ryu_timestamp_ms_t
		status := C.ryu_value_get_timestamp_ms(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_ms value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000000), nil
	case C.RYU_TIMESTAMP_SEC:
		var value C.ryu_timestamp_sec_t
		status := C.ryu_value_get_timestamp_sec(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_sec value with status: %d", status)
		}
		return time.Unix(int64(value.value), 0), nil
	case C.RYU_TIMESTAMP_TZ:
		var value C.ryu_timestamp_tz_t
		status := C.ryu_value_get_timestamp_tz(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get timestamp_tz value with status: %d", status)
		}
		return time.Unix(0, int64(value.value)*1000), nil
	case C.RYU_DATE:
		var value C.ryu_date_t
		status := C.ryu_value_get_date(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get date value with status: %d", status)
		}
		return ryuDateToTime(value), nil
	case C.RYU_INTERVAL:
		var value C.ryu_interval_t
		status := C.ryu_value_get_interval(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get interval value with status: %d", status)
		}
		return ryuIntervalToDuration(value), nil
	case C.RYU_INTERNAL_ID:
		var value C.ryu_internal_id_t
		status := C.ryu_value_get_internal_id(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get internal_id value with status: %d", status)
		}
		return InternalID{TableID: uint64(value.table_id), Offset: uint64(value.offset)}, nil
	case C.RYU_BLOB:
		var value *C.uint8_t
		status := C.ryu_value_get_blob(&ryuValue, &value)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get blob value with status: %d", status)
		}
		defer C.ryu_destroy_blob(value)
		blobSize := C.strlen((*C.char)(unsafe.Pointer(value)))
		blob := C.GoBytes(unsafe.Pointer(value), C.int(blobSize))
		return blob, nil
	case C.RYU_NODE:
		return ryuNodeValueToGoValue(ryuValue)
	case C.RYU_REL:
		return ryuRelValueToGoValue(ryuValue)
	case C.RYU_RECURSIVE_REL:
		return ryuRecursiveRelValueToGoValue(ryuValue)
	case C.RYU_LIST, C.RYU_ARRAY:
		return ryuListValueToGoValue(ryuValue)
	case C.RYU_STRUCT, C.RYU_UNION:
		return ryuStructValueToGoValue(ryuValue)
	case C.RYU_MAP:
		return ryuMapValueToGoValue(ryuValue)
	case C.RYU_DECIMAL:
		var outString *C.char
		status := C.ryu_value_get_decimal_as_string(&ryuValue, &outString)
		if status != C.RyuSuccess {
			return nil, fmt.Errorf("failed to get string value of decimal type with status: %d", status)
		}
		goString := C.GoString(outString)
		C.ryu_destroy_string(outString)
		goDecimal, casting_error := decimal.NewFromString(goString)
		if casting_error != nil {
			return nil, fmt.Errorf("failed to convert decimal value with error: %w", casting_error)
		}
		return goDecimal, casting_error
	default:
		valueString := C.ryu_value_to_string(&ryuValue)
		defer C.ryu_destroy_string(valueString)
		return C.GoString(valueString), fmt.Errorf("unsupported data type with type id: %d. the value is force-casted to string", logicalTypeId)
	}
}

// int128ToBigInt converts a ryu_int128_t to a big.Int in Go.
func int128ToBigInt(value C.ryu_int128_t) (*big.Int, error) {
	var outString *C.char
	status := C.ryu_int128_t_to_string(value, &outString)
	if status != C.RyuSuccess {
		return nil, fmt.Errorf("failed to convert int128 to string with status: %d", status)
	}
	defer C.ryu_destroy_string(outString)
	valueString := C.GoString(outString)
	bigInt := new(big.Int)
	_, success := bigInt.SetString(valueString, 10)
	if !success {
		return nil, fmt.Errorf("failed to convert string to big.Int")
	}
	return bigInt, nil
}

// goMapToRyuStruct converts a map of string to any to a ryu_value representing
// a STRUCT. It returns an error if the map is empty.
func goMapToRyuStruct(value map[string]any) (*C.ryu_value, error) {
	numFields := C.uint64_t(len(value))
	if numFields == 0 {
		return nil, fmt.Errorf("failed to create STRUCT value because the map is empty")
	}
	fieldNames := make([]*C.char, 0, len(value))
	fieldValues := make([]*C.ryu_value, 0, len(value))
	// Sort the keys to ensure the order is consistent.
	// This is useful for creating a LIST of STRUCTs because in Ryu, all the
	// LIST elements must have the same type (i.e., the same order of fields).
	sortedKeys := make([]string, 0, len(value))
	for k := range value {
		sortedKeys = append(sortedKeys, k)
	}
	sort.Strings(sortedKeys)
	for _, k := range sortedKeys {
		fieldNames = append(fieldNames, C.CString(k))
		ryuValue, error := goValueToRyuValue(value[k])
		if error != nil {
			return nil, fmt.Errorf("failed to convert value in the map with error: %w", error)
		}
		fieldValues = append(fieldValues, ryuValue)
		defer C.ryu_value_destroy(ryuValue)
		defer C.free(unsafe.Pointer(C.CString(k)))
	}

	var ryuValue *C.ryu_value
	status := C.ryu_value_create_struct(numFields, &fieldNames[0], &fieldValues[0], &ryuValue)
	if status != C.RyuSuccess {
		return nil, fmt.Errorf("failed to create STRUCT value with status: %d", status)
	}
	return ryuValue, nil
}

// goSliceOfMapItemsToRyuMap converts a slice of MapItem to a ryu_value
// representing a MAP. It returns an error if the slice is empty or if the keys
// in the slice are of different types or if the values in the slice are of
// different types.
func goSliceOfMapItemsToRyuMap(slice []MapItem) (*C.ryu_value, error) {
	numItems := C.uint64_t(len(slice))
	if numItems == 0 {
		return nil, fmt.Errorf("failed to create MAP value because the slice is empty")
	}
	keys := make([]*C.ryu_value, 0, len(slice))
	values := make([]*C.ryu_value, 0, len(slice))
	for _, item := range slice {
		key, error := goValueToRyuValue(item.Key)
		if error != nil {
			return nil, fmt.Errorf("failed to convert key in the slice with error: %w", error)
		}
		keys = append(keys, key)
		defer C.ryu_value_destroy(key)
		value, error := goValueToRyuValue(item.Value)
		if error != nil {
			return nil, fmt.Errorf("failed to convert value in the slice with error: %w", error)
		}
		values = append(values, value)
		defer C.ryu_value_destroy(value)
	}
	var ryuValue *C.ryu_value
	status := C.ryu_value_create_map(numItems, &keys[0], &values[0], &ryuValue)
	if status != C.RyuSuccess {
		return nil, fmt.Errorf("failed to create MAP value with status: %d. please make sure all the keys are of the same type and all the values are of the same type", status)
	}
	return ryuValue, nil
}

// goSliceToRyuList converts a slice of any to a ryu_value representing a LIST.
// It returns an error if the slice is empty or if the values in the slice are of
// different types.
func goSliceToRyuList(slice []any) (*C.ryu_value, error) {
	numItems := C.uint64_t(len(slice))
	if numItems == 0 {
		return nil, fmt.Errorf("failed to create LIST value because the slice is empty")
	}
	values := make([]*C.ryu_value, 0, len(slice))
	for _, item := range slice {
		value, error := goValueToRyuValue(item)
		if error != nil {
			return nil, fmt.Errorf("failed to convert value in the slice with error: %w", error)
		}
		values = append(values, value)
		defer C.ryu_value_destroy(value)
	}
	var ryuValue *C.ryu_value
	status := C.ryu_value_create_list(numItems, &values[0], &ryuValue)
	if status != C.RyuSuccess {
		return nil, fmt.Errorf("failed to create LIST value with status: %d. please make sure all the values are of the same type", status)
	}
	return ryuValue, nil
}

// ryuValueToGoValue converts a Go value to a ryu_value.
func goValueToRyuValue(value any) (*C.ryu_value, error) {
	if value == nil {
		return C.ryu_value_create_null(), nil
	}
	var ryuValue *C.ryu_value
	switch v := value.(type) {
	case bool:
		ryuValue = C.ryu_value_create_bool(C.bool(v))
	case int:
		ryuValue = C.ryu_value_create_int64(C.int64_t(v))
	case int64:
		ryuValue = C.ryu_value_create_int64(C.int64_t(v))
	case int32:
		ryuValue = C.ryu_value_create_int32(C.int32_t(v))
	case int16:
		ryuValue = C.ryu_value_create_int16(C.int16_t(v))
	case int8:
		ryuValue = C.ryu_value_create_int8(C.int8_t(v))
	case uint:
		ryuValue = C.ryu_value_create_uint64(C.uint64_t(v))
	case uint64:
		ryuValue = C.ryu_value_create_uint64(C.uint64_t(v))
	case uint32:
		ryuValue = C.ryu_value_create_uint32(C.uint32_t(v))
	case uint16:
		ryuValue = C.ryu_value_create_uint16(C.uint16_t(v))
	case uint8:
		ryuValue = C.ryu_value_create_uint8(C.uint8_t(v))
	case float64:
		ryuValue = C.ryu_value_create_double(C.double(v))
	case float32:
		ryuValue = C.ryu_value_create_float(C.float(v))
	case string:
		ryuValue = C.ryu_value_create_string(C.CString(v))
	case time.Time:
		if timeHasNanoseconds(v) {
			ryuValue = C.ryu_value_create_timestamp_ns(timeToRyuTimestampNs(v))
		} else {
			ryuValue = C.ryu_value_create_timestamp(timeToRyuTimestamp(v))
		}
	case time.Duration:
		interval := durationToRyuInterval(v)
		ryuValue = C.ryu_value_create_interval(interval)
	case map[string]any:
		return goMapToRyuStruct(v)
	case []MapItem:
		return goSliceOfMapItemsToRyuMap(v)
	case []any:
		return goSliceToRyuList(v)
	default:
		if reflect.TypeOf(value).Kind() == reflect.Slice {
			sliceValue := reflect.ValueOf(value)
			slice := make([]any, sliceValue.Len())
			for i := 0; i < sliceValue.Len(); i++ {
				slice[i] = sliceValue.Index(i).Interface()
			}
			return goSliceToRyuList(slice)
		}
		return nil, fmt.Errorf("unsupported type: %T", v)
	}
	return ryuValue, nil
}
