package main

import (
	"bytes"
	"fmt"
	"go/format"
	"io/ioutil"
	"strings"
	"text/template"

	"go.pedge.io/env"
)

// This generation tool is not meant to be amazing code, it was just continually built and works.
// Really REALLY not good code overall, but the generation either works or does not work.
// If something is broken here, bolttype almost for sure will not compile.

var (
	globalTmplData = &tmplData{
		Types: []*tmplDataType{
			singleTmplDataType("[]byte"),
			singleTmplDataType("string"),
			singleTmplDataType("uint16"),
			singleTmplDataType("uint32"),
			singleTmplDataType("uint64"),
			pairTmplDataType("uint16", "uint16"),
			pairTmplDataType("uint32", "uint32"),
			pairTmplDataType("uint64", "uint64"),
			pairTmplDataType("uint16", "[]byte"),
			pairTmplDataType("uint32", "[]byte"),
			pairTmplDataType("uint64", "[]byte"),
			pairTmplDataType("[]byte", "uint16"),
			pairTmplDataType("[]byte", "uint32"),
			pairTmplDataType("[]byte", "uint64"),
		},
	}
	typeToZeroValue = map[string]string{
		"[]byte": "nil",
		"string": "\"\"",
		"uint16": "0",
		"uint32": "0",
		"uint64": "0",
	}
)

type appEnv struct {
	TmplFilePath string `env:"TMPL_FILE_PATH,required"`
	GoFilePath   string `env:"GO_FILE_PATH,required"`
}

func main() {
	env.Main(do, &appEnv{})
}

func do(appEnvObj interface{}) error {
	appEnv := appEnvObj.(*appEnv)
	tmpl, err := template.ParseFiles(appEnv.TmplFilePath)
	if err != nil {
		return err
	}
	buffer := bytes.NewBuffer(nil)
	if err := tmpl.Execute(buffer, globalTmplData); err != nil {
		return err
	}
	data, err := format.Source(buffer.Bytes())
	if err != nil {
		return err
	}
	return ioutil.WriteFile(appEnv.GoFilePath, data, 0644)
}

type tmplData struct {
	Types []*tmplDataType
}

type tmplDataType struct {
	Name                  string
	Type                  string
	TypedKeyParameters    string
	KeyParameters         string
	TypedValueParameters  string
	ValueParameters       string
	ReturnValueParameters string
	ZeroValue             string
	ToBytesFunc           string
	FromBytesFunc         string
	ReturnParameters      string
}

func singleTmplDataType(t string) *tmplDataType {
	// again, we know len(t) > 0, so this works even though it is not good code
	name := fmt.Sprintf("%s%s", strings.ToUpper(t[0:1]), t[1:])
	toBytesFunc := fmt.Sprintf("%sTobytes", t)
	fromBytesFunc := fmt.Sprintf("bytesTo%s", t)
	if t == "[]byte" {
		name = "Bytes"
		toBytesFunc = ""
		fromBytesFunc = ""
	}
	if t == "string" {
		toBytesFunc = "[]byte"
		fromBytesFunc = "string"
	}
	return &tmplDataType{
		Name:                  name,
		Type:                  t,
		TypedKeyParameters:    fmt.Sprintf("key %s", t),
		KeyParameters:         "key",
		TypedValueParameters:  fmt.Sprintf("value %s", t),
		ValueParameters:       "value",
		ReturnValueParameters: t,
		// bad code again, but it does not really matter, if we get "", then there will be misplaced commas etc and bolttype will not compile
		ZeroValue:     typeToZeroValue[t],
		ToBytesFunc:   toBytesFunc,
		FromBytesFunc: fromBytesFunc,
	}
}

func pairTmplDataType(
	t1 string,
	t2 string,
) *tmplDataType {
	name1 := fmt.Sprintf("%s%s", strings.ToUpper(t1[0:1]), t1[1:])
	name2 := fmt.Sprintf("%s%s", strings.ToUpper(t2[0:1]), t2[1:])
	if t1 == "[]byte" {
		name1 = "Bytes"
	}
	if t2 == "[]byte" {
		name2 = "Bytes"
	}
	name := fmt.Sprintf("%s%s", name1, name2)
	toBytesFunc := fmt.Sprintf("%s%sTobytes", t1, t2)
	fromBytesFunc := fmt.Sprintf("bytesTo%s%s", t1, t2)
	if t1 == "[]byte" {
		toBytesFunc = fmt.Sprintf("bytes%sTobytes", t2)
		fromBytesFunc = fmt.Sprintf("bytesTobytes%s", t2)
	}
	if t2 == "[]byte" {
		toBytesFunc = fmt.Sprintf("%sbytesTobytes", t1)
		fromBytesFunc = fmt.Sprintf("bytesTo%sbytes", t1)
	}
	return &tmplDataType{
		Name:                  name,
		Type:                  fmt.Sprintf("pair(%s,%s)", t1, t2),
		TypedKeyParameters:    fmt.Sprintf("key1 %s, key2 %s", t1, t2),
		KeyParameters:         "key1, key2",
		TypedValueParameters:  fmt.Sprintf("value1 %s, value2 %s", t1, t2),
		ValueParameters:       "value1, value2",
		ReturnValueParameters: fmt.Sprintf("%s, %s", t1, t2),
		ZeroValue:             fmt.Sprintf("%s, %s", typeToZeroValue[t1], typeToZeroValue[t2]),
		ToBytesFunc:           toBytesFunc,
		FromBytesFunc:         fromBytesFunc,
		ReturnParameters:      "value1, value2",
	}
}
