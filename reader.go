package gospss

import (
	"bufio"
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"errors"
	"io"
	"math"
	"strconv"
	"strings"
	"unsafe"
)

var (
	err                 error
	ErrNotValidSPSSFile = errors.New("Not a valid IBM SPSS Statistics file.")
)

// A Reader reads data from an IBM SPSS Statistics encoded system file
//
// As returned by NewReader blabla
type Reader struct {
	// Endianess indicates the byte order, default is set to binary.LittleEndian.
	// The package can not infer this on it's own so it has to be supplied if the
	// file has a different byte order than Little Endian.
	// Have never seen, but heard it's possible to have BigEndian.
	endianess binary.ByteOrder
	// Using bufio reader to buffer through the data file.
	r *bufio.Reader
	// header is reading in the metadata of the file.
	header *Header
	// zlib is to see if the file is zlib compressed.
	zlib bool
	// If the data file i zlib compressed we will push the data in to an
	// uncompressed reader.
	uncompressed *bytes.Reader
}

// NewReader returns a new Reader that reads from r
func NewReader(r io.Reader) (*Reader, error) {
	sav := &Reader{
		// Set default endianess to Little Endian as it is most commonly used.
		endianess: machineEndianess(),
		r:         bufio.NewReader(r),
	}
	sav.header, err = sav._header()
	if err != nil {
		return nil, err
	}
	return sav, nil
}

// Change endianess, can be binary.LittleEndian (most common) or binary.BigEndian
func (r *Reader) ChangeEndianess(endianess binary.ByteOrder) {
	r.endianess = endianess
}

// Header contains all the raw IBM SPSS Statistics metadata. It is fairly unstructured
type Header struct {
	Fileheader              *fileHeader
	Variable                []*variabler
	ValueLabel              []*valueLabel
	Documents               *documents
	MachineIntegerInfo      *machineIntegerInfo
	MachineFloatingPoint    *machineFloatingPointInfo
	MultipleResponseSetsOld *multipleResponseSets
	ExtraProductInfo        *extraProductInfo
	VariableDisplay         *variableDisplay
	LongVariableNames       *longVariableNames
	VeryLongString          *veryLongString
	ExtendedNCasesInfo      *extendedNumberOfCases
	DataAttributes          *dataAttributes
	VariableAttributes      *dataAttributes
	MultipleResponseSetsNew *multipleResponseSets
	CharacterEncoding       *characterEncoding
	LongStringValueLabels   *longStringValueLabels
	LongStringMissingValues *longStringMissingValues
	DictionaryTermination   *dictionaryTermination
	ZLibDataHeader          *zLibDataHeader
	ZLibDataTrailer         *zLibDataTrailer
	metaData                []*Variable
}

// Constants used to determine the type of record.
var (
	variableRecord                = []int32{2}
	valueLabelsRecord             = []int32{3}
	documentRecord                = []int32{6}
	machineIntegerInfoRecord      = []int32{7, 3, 4}
	machineFloatingPointRecord    = []int32{7, 4, 8}
	multipleResponseSetsOld       = []int32{7, 7, 1}
	extraProductInfoRecord        = []int32{7, 10, 1}
	variableDisplayRecord         = []int32{7, 11, 4}
	longVariableNamesRecord       = []int32{7, 13, 1}
	veryLongStringRecord          = []int32{7, 14, 1}
	extendedNCasesRecord          = []int32{7, 16, 8}
	dataFileAttributesRecord      = []int32{7, 17, 1}
	variableAttributesRecord      = []int32{7, 18, 1}
	multipleResponseSetsNew       = []int32{7, 19, 1}
	characterEncodingRecord       = []int32{7, 20, 1}
	longStringValueLabelsRecord   = []int32{7, 21, 1}
	longStringMissingValuesRecord = []int32{7, 22, 1}
	dictionaryTerminationRecord   = []int32{999, 0}
)

// MetaData returns Human friendly meta data in form of a list of the Variable struct.
func (r *Reader) MetaData() []*Variable {
	return r.header.metaData
}

// Data is the raw data type.
type Row []interface{}

// ReadAll returns a pointer to an Spss struct and an error.
func (r *Reader) ReadAll() ([]Row, error) {
	// Read data
	var rows []Row
	for {
		row, err := r.readDataRecord()
		if err == io.EOF {
			return rows, nil
		}
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
	}
}

// Read returns a pointer to an list of data and an error.
func (r *Reader) Read() (Row, error) {
	// Read data
	row, err := r.readDataRecord()
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, io.EOF
	}
	return row, nil
}

// HeaderData returns all raw header data from the IBM SPSS Statistics file.
func (r *Reader) Header() *Header {
	return r.header
}

// Reads the Metadata of the SPSS file only.
func (r *Reader) _header() (*Header, error) {
	h := new(Header)
	// Reading the metadata of the spss file.
	h.Fileheader, err = r.readFileheader()
	if err != nil {
		return nil, err
	}
	// Checks that it is an IBM SPSS Statistics file.
	if !((h.Fileheader.recType == "$FL2") || (h.Fileheader.recType == "$FL3")) {
		return nil, ErrNotValidSPSSFile
	}
	h.Variable, err = r.readVariabler()
	if err != nil {
		return nil, err
	}
	metadata := true
	for metadata {
		switch {
		case r.checkNextRecord(valueLabelsRecord):
			h.ValueLabel, err = r.readValueLabels()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(documentRecord):
			h.Documents, err = r.readDocuments()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(machineIntegerInfoRecord):
			h.MachineIntegerInfo, err = r.readMachineIntegerInfo()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(machineFloatingPointRecord):
			h.MachineFloatingPoint, err = r.readMachineFloatingPointInfo()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(multipleResponseSetsOld):
			h.MultipleResponseSetsOld, err = r.readMultipleResponseSets()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(extraProductInfoRecord):
			h.ExtraProductInfo, err = r.readExtraProductInfo()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(variableDisplayRecord):
			h.VariableDisplay, err = r.readVariableDisplay()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(longVariableNamesRecord):
			h.LongVariableNames, err = r.readLongVariableNames()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(veryLongStringRecord):
			h.VeryLongString, err = r.readVeryLongString()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(extendedNCasesRecord):
			h.ExtendedNCasesInfo, err = r.readExtendedNumberOfCases()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(dataFileAttributesRecord):
			h.DataAttributes, err = r.readDataAttributes()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(variableAttributesRecord):
			h.VariableAttributes, err = r.readDataAttributes()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(multipleResponseSetsNew):
			h.MultipleResponseSetsNew, err = r.readMultipleResponseSets()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(characterEncodingRecord):
			h.CharacterEncoding, err = r.readCharacterEnoding()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(longStringValueLabelsRecord):
			h.LongStringValueLabels, err = r.readLongStringValueLabels()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(longStringMissingValuesRecord):
			h.LongStringMissingValues, err = r.readLongStringMissingValues()
			if err != nil {
				return nil, err
			}
		case r.checkNextRecord(dictionaryTerminationRecord):
			h.DictionaryTermination, err = r.readDictionaryTermination()
			if err != nil {
				return nil, err
			}
			metadata = false
		default:
			_, err := r.r.Discard(1)
			if err == io.EOF {
				metadata = false
			}
		}
	}

	// Uncompress the data if it is a zsav file
	if h.Fileheader.compression == 2 {
		h.ZLibDataHeader, err = r.readZLibHeader()
		if err != nil {
			return nil, err
		}

		rc, err := zlib.NewReader(r.r)
		if err != nil {
			return nil, err
		}
		defer rc.Close()

		var uncompressed []byte
		for {
			tb := make([]byte, 32768)
			_, err = rc.Read(tb)
			if err == io.EOF {
				uncompressed = append(uncompressed, tb...)
				break
			}
			if err != nil {
				return nil, err
			}
			uncompressed = append(uncompressed, tb...)
		}
		r.uncompressed = bytes.NewReader(uncompressed)

		h.ZLibDataTrailer, err = r.readZLibTrailer()
		if err != nil {
			return nil, err
		}
		r.zlib = true
	}
	// Construct the meta data.
	h.metaData = r.constrVariables(h)

	return h, nil
}

// Helper function for ReadAll to peek what type of record is being read next.
func (r *Reader) checkNextRecord(input []int32) bool {
	peek, err := r.r.Peek(len(input) * 4)
	if err != nil {
		return false
	}
	buf := new(bytes.Buffer)
	for i := 0; i < len(input); i++ {
		binary.Write(buf, r.endianess, input[i])
	}
	return bytes.Equal(peek, buf.Bytes())
}

// readBytes returns n number of bytes is a slice of byte and an error.
func (r *Reader) readBytes(n int) ([]byte, error) {
	lb := make([]byte, n)
	if r.zlib {
		_, err = r.uncompressed.Read(lb)
	} else {
		_, err = r.r.Read(lb)
	}
	if err != nil {
		return nil, err
	}
	return lb, nil
}

// readString returns n number of characters in a string and an error.
func (r *Reader) readString(n int) (string, error) {
	b, err := r.readBytes(n)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// readInt32 reads 4 bytes and returns an Int32 together with an error
func (r *Reader) readInt32() (int32, error) {
	var i32 int32
	b, err := r.readBytes(4)
	if err != nil {
		return i32, err
	}
	binary.Read(bytes.NewReader(b), r.endianess, &i32)
	return i32, nil
}

// readFlt64 reads 8 bytes and returns a float64 together with an error
func (r *Reader) readFlt64() (float64, error) {
	var f64 float64
	b, err := r.readBytes(8)
	if err != nil {
		return f64, err
	}
	binary.Read(bytes.NewReader(b), r.endianess, &f64)
	return f64, nil
}

// readInt64 reads 8 bytes and returns an int64 together with an error.
func (r *Reader) readInt64() (int64, error) {
	var i64 int64
	b, err := r.readBytes(8)
	if err != nil {
		return i64, err
	}
	binary.Read(bytes.NewReader(b), r.endianess, &i64)
	return i64, nil
}

// An IBM SPSS Statistics file begins with a file header.
type fileHeader struct {
	// Record type code, either $FL2 for IBM SPSS Statistics files with
	// uncompressed data or data compressed with simple bytecode compression,
	// or $FL3 for IBM SPSS Statistics files with ZLIB compressed data.
	recType string

	// Product identification string. This always begins with the characters
	// @@(#) SPSS DATA FILE.
	prodName string

	// Normally set to 2, although a few IBM SPSS Statistics files have been spotted in
	// the wild with a value of 3 here. Can be used to determine endianness.
	layoutCode int32

	// 	Number of data elements per case.  This is the number of variables,
	// except that long string variables add extra data elements (one for every
	// 8 characters after the first 8).
	nominalCaseSize int32

	// Set to 0 if the data in the file is not compressed, 1 if the data is
	// compressed with simple bytecode compression, 2 if the data is ZLIB
	// compressed.  This field has value 2 if and only if recType is
	// $FL3.
	compression int32

	// If one of the variables in the data set is used as a weighting
	// variable, set to the dictionary index of that variable, plus 1.
	// Otherwise, set to 0.
	weightIndex int32

	// Set to the number of cases in the file if it is known, or -1 otherwise.
	ncases int32

	// Compression bias, ordinarily set to 100. Only integers between
	// 1 - bias and 251 - bias can be compressed.
	bias float64

	// Date of creation of the system file, in "dd mmm yy"
	// format, with the month as standard English abbreviations, using an
	// initial capital letter and following with lowercase.
	creationDate string

	// Time of creation of the system file, in "hh:mm:ss"
	// format and using 24-hour time.
	creationTime string

	// File label declared by the user.
	fileLabel string

	// Ignored padding bytes to make the structure a multiple of 32 bits in
	// length. Set to zeros.
	padding string
}

// readFileheader reads the fileheader and returns a pointer to a fileheader and an error.
func (r *Reader) readFileheader() (*fileHeader, error) {
	fh := new(fileHeader)
	if fh.recType, err = r.readString(4); err != nil {
		return nil, err
	}
	if fh.prodName, err = r.readString(60); err != nil {
		return nil, err
	}
	if fh.layoutCode, err = r.readInt32(); err != nil {
		return nil, err
	}
	if fh.nominalCaseSize, err = r.readInt32(); err != nil {
		return nil, err
	}
	if fh.compression, err = r.readInt32(); err != nil {
		return nil, err
	}
	if fh.weightIndex, err = r.readInt32(); err != nil {
		return nil, err
	}
	if fh.ncases, err = r.readInt32(); err != nil {
		return nil, err
	}
	if fh.bias, err = r.readFlt64(); err != nil {
		return nil, err
	}
	if fh.creationDate, err = r.readString(9); err != nil {
		return nil, err
	}
	if fh.creationTime, err = r.readString(8); err != nil {
		return nil, err
	}
	if fh.fileLabel, err = r.readString(64); err != nil {
		return nil, err
	}
	if fh.padding, err = r.readString(3); err != nil {
		return nil, err
	}
	return fh, nil
}

// An IBM SPSS Statistics must contains a variable record.
type variabler struct {
	// Record type code. Always set to 2.
	recType int32

	// Variable type code. Set to 0 for a numeric variable. For a short
	// string variable or the first part of a long string variable, this is set
	// to the width of the string. For the second and subsequent parts of a
	// long string variable, set to -1, and the remaining fields in the
	// structure are ignored.
	tpe int32

	// If this variable has a variable label, set to 1; otherwise, set to 0.
	hasVarLabel int32

	// If the variable has no missing values, set to 0. If the variable has
	// one, two, or three discrete missing values, set to 1, 2, or 3,
	// respectively. If the variable has a range for missing variables, set to
	// -2; if the variable has a range for missing variables plus a single
	// discrete value, set to -3.
	nMissingValues int32

	// The print and write members of sysfile_variable are output
	// formats coded into int32 types. The least-significant byte
	// of the int32 represents the number of decimal places, and the
	// next two bytes in order of increasing significance represent field width
	// and format type, respectively. The most-significant byte is not
	// used and should be set to zero.
	print *pw
	write *pw

	// Variable name. The variable name must begin with a capital letter or
	// the at-sign. Subsequent characters may also be digits, octothorpes (#),
	// dollar signs ($), underscores (_), or full stops (.). The variable name
	// is padded on the right with spaces.
	char string

	// This field is present only if hasVarLabel is set to 1. It is
	// set to the length, in characters, of the variable label. The
	// documented maximum length varies from 120 to 255 based on SPSS
	// version, but some files have been seen with longer labels.
	labelLen int32

	// This field is present only if @code{has_var_label} is set to 1. It has
	// length labelLen, rounded up to the nearest multiple of 32 bits.
	// The first labelLen characters are the variable's variable label.
	label string

	// This field is present only if nMissingValues is nonzero. It
	// has the same number of 8-byte elements as the absolute value of
	// nMissingValues. Each element is interpreted as a number for
	// numeric variables (with HIGHEST and LOWEST indicated as described in
	// the chapter introduction). For string variables of width less than 8
	// bytes, elements are right-padded with spaces; for string variables
	// wider than 8 bytes, only the first 8 bytes of each missing value are
	// specified, with the remainder implicitly all spaces.

	// For discrete missing values, each element represents one missing
	// value. When a range is present, the first element denotes the minimum
	// value in the range, and the second element denotes the maximum value
	// in the range. When a range plus a value are present, the third
	// element denotes the additional discrete missing value.
	missingValues []float64
}

type pw struct {
	// The least significant byte indicates the decimal value.
	decimal int32
	// The next bytes indicates the width of the variable.
	width int32
	// The next bytes indicates the type of the variable.
	tpe int32
}

// readVariable reads the upcoming bytes and returns a list of variable
// and an error
func (r *Reader) readVariabler() ([]*variabler, error) {
	var v []*variabler

	// utility function for this function only.
	printwrite := func(b []byte) (*pw, error) {
		var v3 int32
		if int(b[3]) == 0 {
			v3 = int32(b[2])
		} else {
			v3 = int32(b[2])*10 + int32(b[3])
		}
		return &pw{
			decimal: int32(b[0]),
			width:   int32(b[1]),
			tpe:     v3,
		}, nil
	}

	calcPadding := func(i int) int {
		switch i % 4 {
		case 1:
			return 3
		case 2:
			return 2
		case 3:
			return 1
		default:
			return 0
		}
	}

	for {
		vr := new(variabler)
		if vr.recType, err = r.readInt32(); err != nil {
			return nil, err
		}
		if vr.tpe, err = r.readInt32(); err != nil {
			return nil, err
		}
		if vr.hasVarLabel, err = r.readInt32(); err != nil {
			return nil, err
		}
		if vr.nMissingValues, err = r.readInt32(); err != nil {
			return nil, err
		}
		b1, err := r.readBytes(4)
		if err != nil {
			return nil, err
		}
		vr.print, err = printwrite(b1)
		if err != nil {
			return nil, err
		}
		b2, err := r.readBytes(4)
		if err != nil {
			return nil, err
		}
		vr.write, err = printwrite(b2)
		if err != nil {
			return nil, err
		}
		if vr.char, err = r.readString(8); err != nil {
			return nil, err
		}
		vr.char = strings.TrimSpace(vr.char)
		if vr.hasVarLabel == 1 {
			if vr.labelLen, err = r.readInt32(); err != nil {
				return nil, err
			}
			add := calcPadding(int(vr.labelLen))
			if vr.label, err = r.readString(int(vr.labelLen) + add); err != nil {
				return nil, err
			}
		}
		if vr.nMissingValues > 0 {
			for i := 0; i < int(vr.nMissingValues); i++ {
				mv, err := r.readFlt64()
				if err != nil {
					return nil, err
				}
				vr.missingValues = append(vr.missingValues, mv)
			}
		}
		// TODO: find out why I need this here.
		r.r.Peek(2000)
		v = append(v, vr)
		if !r.checkNext(2) {
			return v, nil
		}
	}
}

// If present, the Value labels record of the IBM SPSS Statistics file.
type valueLabel struct {
	// Record type. Always set to 3.
	recType int32

	// Number of value labels present in this record.
	labelCount int32

	// List of struct of value label key/value pairs.
	labels []*vl

	// Struct that contains the variables that share the value label structure.
	vlvr *vlvr
}

// Value Label key/value pairs
type vl struct {
	// A numeric value or a short string value padded as necessary to 8 bytes
	// in length. Its type and width cannot be determined until the
	// following value label variables record (see below) is read.
	value float64

	// The label's length, in bytes. The documented maximum length varies
	// from 60 to 120 based on SPSS version.
	labelLen int32

	// labelLen bytes of the actual label, followed by up to 7 bytes
	// of padding to bring label and labelLen together to a
	// multiple of 8 bytes in length.
	label string
}

// List of variable that uses the value label.
type vlvr struct {
	// Record type. Always set to 4.
	recType int32

	// Number of variables that the associated value labels from the value
	// label record are to be applied.
	varCount int32

	// A list of dictionary indexes of variables to which to apply the value
	// labels index. There are varCount elements.
	// String variables wider than 8 bytes may not be specified in this list.
	vars []int32
}

// readValueLabels returns a list of pointers to a valuelabel and an error.
func (r *Reader) readValueLabels() ([]*valueLabel, error) {
	var m []*valueLabel
	for {
		// TODO: Find out why this i needed.
		r.r.Peek(2000)
		n := new(valueLabel)
		n.recType, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		n.labelCount, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		for i := int32(0); i < n.labelCount; i++ {
			v1 := new(vl)
			// TODO: Make a check for string or float, value should be stored as a string and not a float.
			v1.value, err = r.readFlt64()
			if err != nil {
				return nil, err
			}
			b1, err := r.readBytes(1)
			if err != nil {
				return nil, err
			}
			v1.labelLen = calcLen(int(b1[0]), 8)
			v1.label, err = r.readString(int(v1.labelLen))
			if err != nil {
				return nil, err
			}
			n.labels = append(n.labels, v1)
		}
		valrec := new(vlvr)
		valrec.recType, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		valrec.varCount, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		for i := 0; i < int(valrec.varCount); i++ {
			v2, err := r.readInt32()
			if err != nil {
				return nil, err
			}
			valrec.vars = append(valrec.vars, v2)
		}
		n.vlvr = valrec
		if !r.checkNext(3) {
			break
		}
		m = append(m, n)
	}
	return m, nil
}

// Helper function for readVariable() and readValueLabels()
func (r *Reader) checkNext(i int) bool {
	b, err := r.r.Peek(4)
	if err != nil {
		return false
	}
	var val int32
	binary.Read(bytes.NewReader(b), r.endianess, &val)
	return val == int32(i)
}

// Helper function for readValueLabels()
func calcLen(i, split int) int32 {
	return int32((i/split+1)*split - 1)
}

// If present, the Document record of the IBM SPSS Statistics file.
type documents struct {
	// Record type. Always set to 6.
	recType int32

	// Number of lines of documents present.
	nLines int32

	// Document lines. The number of elements is defined by nLines.
	// Lines shorter than 80 characters are padded on the right with spaces.
	char []string
}

// readDocuments returns a pointer to a documents and an error.
func (r *Reader) readDocuments() (*documents, error) {
	doc := new(documents)
	doc.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	doc.nLines, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	for i := 0; i < int(doc.nLines); i++ {
		char, err := r.readString(80)
		if err != nil {
			return nil, err
		}
		doc.char = append(doc.char, char)
	}
	return doc, nil
}

// If present, the machine integer record of the IBM SPSS Statistics file.
type machineIntegerInfo struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 3.
	subtype int32

	// Size of each piece of data in the data part, in bytes. Always set to 4.
	size int32

	// Number of pieces of data in the data part. Always set to 8.
	count int32

	// SPSS Major version number.
	versionMajor int32

	// SPSS Minor version number.
	versionMinor int32

	// SPSS Revision version number.
	versionRevision int32

	// Machine code. Most often set to -1, but other values may appear.
	machineCode int32

	// Floating point representation code. For IEEE 754 systems this is 1.
	// IBM 370 sets this to 2, and DEC VAX E to 3.
	floatingPointRep int32

	// Compression code. Always set to 1, regardless of whether or how the
	// file is compressed.
	compressionCode int32

	// Machine endianness. 1 indicates big-endian, 2 indicates little-endian.
	endianess int32

	// Character code. The following values have been actually observed in system files:

	// 1: EBCDIC
	// 2: 7-bit ASCII
	// 3: 8-bit ``ASCII''
	// 4: DEC Kanji
	// 1250: windows-1250
	// 1252: windows-1252
	// 28591: ISO 8859-1
	// 65001: UTF-8
	characterCode int32
}

// readMachineIntegerInfo returns a pointer to a documents and an error.
func (r *Reader) readMachineIntegerInfo() (*machineIntegerInfo, error) {
	m := new(machineIntegerInfo)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.versionMajor, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.versionMinor, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.versionRevision, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.machineCode, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.floatingPointRep, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.compressionCode, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.endianess, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.characterCode, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If present, the machine floating point record of the IBM SPSS Statistics file.
type machineFloatingPointInfo struct {
	// Record type. Always set to 7.
	recType int32

	// Record sub type. Always set to 4.
	subtype int32

	// Size of each piece of data in the data part, in bytes. Always set to 8.
	size int32

	// Number of pieces of data in the data part. Always set to 3.
	count int32

	// The system missing value, the value used for HIGHEST in missing
	// values, and the value used for LOWEST in missing values, respectively.
	sysmis  float64
	highest float64
	lowest  float64
}

// readMachineFloatingPointInfo returns a pointer to a documents and an error.
func (r *Reader) readMachineFloatingPointInfo() (*machineFloatingPointInfo, error) {
	m := new(machineFloatingPointInfo)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.sysmis, err = r.readFlt64()
	if err != nil {
		return nil, err
	}
	m.highest, err = r.readFlt64()
	if err != nil {
		return nil, err
	}
	m.lowest, err = r.readFlt64()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If present, the multiple resonse sets record of the IBM SPSS Statistics file.
type multipleResponseSets struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Set to 7 for records that describe multiple response
	// sets understood by SPSS before version 14, or to 19 for records that
	// describe dichotomy sets that use the CATEGORYLABELS=COUNTEDVALUES
	// feature added in version 14.
	subtype int32

	// The size of each element in the multiple response sets member. Always set to 1.
	size int32

	// The total number of bytes in mrsets.
	count int32

	// Zero or more line feeds (byte 0x0a), followed by a series of multiple
	// response sets, each of which consists of the following:

	// The set's name (an identifier that begins with ($)), in mixed
	// upper and lower case.

	// An equals sign (=).

	// (C) for a multiple category set, (D) for a multiple
	// dichotomy set with CATEGORYLABELS=VARLABELS, or (E) for a
	// multiple dichotomy set with CATEGORYLABELS=COUNTEDVALUES.

	// For a multiple dichotomy set with CATEGORYLABELS=COUNTEDVALUES, a
	// space, followed by a number expressed as decimal digits, followed by a
	// space. If LABELSOURCE=VARLABEL was specified on MRSETS, then the
	// number is 11; otherwise it is 1.

	// For either kind of multiple dichotomy set, the counted value, as a
	// positive integer count specified as decimal digits, followed by a
	// space, followed by as many string bytes as specified in the count.  If
	// the set contains numeric variables, the string consists of the counted
	// integer value expressed as decimal digits. If the set contains string
	// variables, the string contains the counted string value. Either way,
	// the string may be padded on the right with spaces (older versions of
	// SPSS seem to always pad to a width of 8 bytes; newer versions don't).

	// A space.

	// The multiple response set's label, using the same format as for the
	// counted value for multiple dichotomy sets. A string of length 0 means
	// that the set does not have a label. A string of length 0 is also
	// written if LABELSOURCE=VARLABEL was specified.

	// A space.

	// The short names of the variables in the set, converted to lowercase,
	// each separated from the previous by a single space.
	// Even though a multiple response set must have at least two variables,
	// some system files contain multiple response sets with no variables or
	// one variable. The source and meaning of these multiple response sets is
	// unknown.

	// One line feed (byte 0x0a). Sometimes multiple, even hundreds, of line
	// feeds are present.
	mrsets string
}

// readMultipleResponseSets returns a pointer to a multipleresponsesets and an error.
func (r *Reader) readMultipleResponseSets() (*multipleResponseSets, error) {
	m := new(multipleResponseSets)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.mrsets, err = r.readString(int(m.count))
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If present, the extra product info record of the IBM SPSS Statistics file.
type extraProductInfo struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 10.
	subtype int32

	// The size of each element in the info member. Always set to 1.
	size int32

	// The total number of bytes in info.
	count int32

	// A text string.
	info string
}

// readExtraProductInfo returns a pointer to an extraproductinfo and an error.
func (r *Reader) readExtraProductInfo() (*extraProductInfo, error) {
	m := new(extraProductInfo)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.info, err = r.readString(int(m.count))
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If present, the variable display record of the IBM SPSS Statistics file.
type variableDisplay struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 11.
	subtype int32

	// The size of int32. Always set to 4.
	size int32

	// The number of sets of variable display parameters (ordinarily the
	// number of variables in the dictionary), times 2 or 3.
	count int32

	// List of the struct display.
	display []*display
}

type display struct {
	// The measurement type of the variable:
	// 1: Nominal scale
	// 2: Ordinal scale
	// 3: Continuous scale
	measure int32

	// The width of the display column for the variable in characters.
	// This field is present if count is 3 times the number of
	// variables in the dictionary. It is omitted if count is 2 times
	// the number of variables.
	width int32

	// The alignment of the variable for display purposes:
	// 0: Left aligned
	// 1: Right aligned
	// 2: Center aligned
	alignment int32
}

// readVariableDisplay returns a pointer to a variabledisplay and an error.
func (r *Reader) readVariableDisplay() (*variableDisplay, error) {
	m := new(variableDisplay)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	//m.count is the number of records times 2 or 3, only seen 3 in practise.
	//TODO: make test to check if it is times 2 or 3.
	for i := 0; i < int(m.count/3); i++ {
		d := new(display)
		d.measure, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		d.width, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		d.alignment, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		m.display = append(m.display, d)
	}
	return m, nil
}

// If present, the long variable names record of the IBM SPSS Statistics file.
type longVariableNames struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 13.
	subtype int32

	// The size of each element in the varNamePairs member. Always set to 1.
	size int32

	// The total number of bytes in varNamePairs.
	count int32

	// List of varNamePairs structs
	varNamePairs []*varNamePairs
}

// A list of key/value tuples, where key is the name
// of a variable, and value is its long variable name.
// Thekey field is at most 8 bytes long and must match the
// name of a variable which appears in the variable record.
// The key and value fields are separated by a (=) byte.
// Each tuple is separated by a byte whose value is 09. There is no
// trailing separator following the last tuple.
// The total length is count bytes.
type varNamePairs struct {
	key string

	// The value field is at most 64 bytes long.
	value string
}

// readLongVariableNames returns a pointer to a longvariablenames and an error.
func (r *Reader) readLongVariableNames() (*longVariableNames, error) {
	m := new(longVariableNames)
	// TODO: Look in to the peek.
	r.r.Peek(2000)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	b1, err := r.readBytes(int(m.count))
	if err != nil {
		return nil, err
	}
	b2 := bytes.Split(b1, []byte{9})
	for _, s := range b2 {
		pair := new(varNamePairs)
		tuple := bytes.Split(s, []byte{61})
		pair.key = string(tuple[0])
		pair.value = string(tuple[1])
		m.varNamePairs = append(m.varNamePairs, pair)
	}
	return m, nil
}

// If present, the very long string record of the IBM SPSS Statistics file.
type veryLongString struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 14.
	subtype int32

	// The size of each element in the stringLength member. Always set to 1.
	size int32

	// The total number of bytes in stringLength.
	count int32

	// List of stringLength struct.
	stringLength []*stringLength
}

// A list of key/value tuples, where key is the name
// of a variable, and value is its length.
// The key field is at most 8 bytes long and must match the
// name of a variable which appears in the variable record.

// The key and value fields are separated by a (=) byte.
// Tuples are delimited by a two-byte sequence @{00, 09@}.
// After the last tuple, there may be a single byte 00, or @{00, 09@}.
// The total length is count bytes.
type stringLength struct {
	key string

	// The @var{value} field is exactly 5 bytes long. It is a zero-padded,
	// ASCII-encoded string that is the length of the variable.
	value string
}

// readVeryLongString returns a pointer to a verylongstring and an error.
func (r *Reader) readVeryLongString() (*veryLongString, error) {
	m := new(veryLongString)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	b1, err := r.readBytes(int(m.count))
	if err != nil {
		return nil, err
	}
	b2 := bytes.Split(b1, []byte{0, 9})
	for _, s := range b2 {
		if len(s) < 1 {
			continue
		}
		pair := new(stringLength)
		tuple := bytes.Split(s, []byte{61})
		pair.key = string(tuple[0])
		pair.value = string(tuple[1])
		m.stringLength = append(m.stringLength, pair)
	}
	return m, nil
}

// If present, the character encoding record of the IBM SPSS Statistics file.
type characterEncoding struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 20.
	subtype int32

	// The size of each element in the encoding member. Always set to 1.
	size int32

	// The total number of bytes in encoding.
	count int32

	// The name of the character encoding. Normally this will be an official
	// IANA character set name or alias.
	encoding string
}

// readCharacterEnoding returns a pointer to a characterencoding and an error.
func (r *Reader) readCharacterEnoding() (*characterEncoding, error) {
	m := new(characterEncoding)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.encoding, err = r.readString(int(m.count))
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If present, the long string value labels record of the IBM SPSS Statistics file.
type longStringValueLabels struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 21.
	subtype int32

	// Always set to 1.
	size int32

	// The number of bytes following the header until the next header.
	count int32

	// Struct of value label pairs
	valueLabelPairs []*valueLabelPairs
}

type valueLabelPairs struct {
	// The number of bytes in the name of the variable that has long string
	// value labels, plus the variable name itself, which consists of exactly
	// varNameLen bytes. The variable name is not padded to any
	// particular boundary, nor is it null-terminated.
	varNameLen int32
	varName    string

	// The width of the variable, in bytes, which will be between 9 and 32767.
	varWidth int32

	// The long string labels themselves. The longLabels array contains
	// exactly nLabels elements, each of which has the following
	// substructure:
	nLabels    int32
	longLabels []*longLabels
}

type longLabels struct {
	// The string value being labeled. valueLen is the number of
	// bytes in value; it is equal to varWidth. The value array
	// is not padded or null-terminated.
	valueLen int32
	value    string

	// The label for the string value. labelLen, which must be
	// between 0 and 120, is the number of bytes in label. The
	// label array is not padded or null-terminated.
	labelLen int32
	label    string
}

// readLongStringValueLabels returns a pointer to a longstringvaluelabels and an error.
func (r *Reader) readLongStringValueLabels() (*longStringValueLabels, error) {
	m := new(longStringValueLabels)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	var ns []*valueLabelPairs
	for i := 0; i < int(m.count); {
		n := new(valueLabelPairs)
		n.varNameLen, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		i += 4
		n.varName, err = r.readString(int(n.varNameLen))
		if err != nil {
			return nil, err
		}
		i += int(n.varNameLen)
		n.varWidth, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		i += 4
		n.nLabels, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		i += 4
		var os []*longLabels
		for j := 0; j < int(n.nLabels); j++ {
			o := new(longLabels)
			o.valueLen, err = r.readInt32()
			if err != nil {
				return nil, err
			}
			i += 4
			o.value, err = r.readString(int(o.valueLen))
			if err != nil {
				return nil, err
			}
			i += int(o.valueLen)
			o.labelLen, err = r.readInt32()
			if err != nil {
				return nil, err
			}
			i += 4
			o.label, err = r.readString(int(o.labelLen))
			if err != nil {
				return nil, err
			}
			i += int(o.labelLen)
			os = append(os, o)
		}
		n.longLabels = os
		ns = append(ns, n)
	}
	m.valueLabelPairs = ns
	return m, nil
}

// If present, the long string missing values record of the IBM SPSS Statistics file.
type longStringMissingValues struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 22.
	subtype int32

	// Always set to 1.
	size int32

	// The number of bytes following the header until the next header.
	count int32

	// List of missings struct.
	missings []*missings
}

type missings struct {
	// The number of bytes in the name of the long string variable that has
	// missing values, plus the variable name itself, which consists of
	// exactly varNameLen bytes.  The variable name is not padded to
	// any particular boundary, nor is it null-terminated.
	varNameLen int32
	varName    string

	// The number of missing values, either 1, 2, or 3.  (This is, unusually,
	// a single byte instead of a 32-bit number.)
	nMissingValues int32

	// List of missingValues struct.
	missingValues []*missingValues
}

type missingValues struct {
	// The length of the missing value string, in bytes.  This value should
	// be 8, because long string variables are at least 8 bytes wide (by
	// definition), only the first 8 bytes of a long string variable's
	// missing values are allowed to be non-spaces, and any spaces within the
	// first 8 bytes are included in the missing value here.
	valueLen int32

	// The missing value string, exactly valueLen bytes, without
	// any padding or null terminator.
	value string
}

// readLongStringMissingValues returns a pointer to a longstringmissingvalues and an error.
func (r *Reader) readLongStringMissingValues() (*longStringMissingValues, error) {
	m := new(longStringMissingValues)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	var ns []*missings
	for i := 0; i < int(m.count); {
		n := new(missings)
		n.varNameLen, err = r.readInt32()
		if err != nil {
			return nil, err
		}
		i += 4
		n.varName, err = r.readString(int(n.varNameLen))
		if err != nil {
			return nil, err
		}
		i += int(n.varNameLen)
		// TODO: Check if value is single byte or int32 before assignment
		b1, err := r.readBytes(1)
		if err != nil {
			return nil, err
		}
		n.nMissingValues = int32(b1[0])
		i++
		var os []*missingValues
		for j := 0; j < int(n.nMissingValues); j++ {
			o := new(missingValues)
			o.valueLen, err = r.readInt32()
			if err != nil {
				return nil, err
			}
			o.value, err = r.readString(int(o.valueLen))
			if err != nil {
				return nil, err
			}
			os = append(os, o)
		}
		n.missingValues = os
		ns = append(ns, n)
	}
	m.missings = ns
	return m, nil
}

// If present, the data attributes record of the IBM SPSS Statistics file.
type dataAttributes struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 17 for a data file attribute record or
	// to 18 for a variable attributes record.
	subtype int32

	// The size of each element in the attributes member. Always set to 1.
	size int32

	// The total number of bytes in attributes.
	count int32

	// The attributes, in a text-based format.
	attributes string

	// Used in the variable attributes to extract variables and their role.
	roles []*role
}

type role struct {
	// Name of the variable.
	name string
	// Role of the variable.
	// 0: Input
	// 1: output
	// 2: Both
	// 3: None
	// 4: Partition
	// 5: Split
	role int
}

// readDataAttributes returns a pointer to a dataattributes and an error.
func (r *Reader) readDataAttributes() (*dataAttributes, error) {
	m := new(dataAttributes)
	r.r.Peek(2102)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.attributes, err = r.readString(int(m.count))
	if err != nil {
		return nil, err
	}
	if m.subtype == 18 {
		m.attributes = strings.Replace(m.attributes, "\x0A", "", -1)
		atr := strings.Split(m.attributes, "/")
		var roles []*role
		for _, a := range atr {
			r := new(role)
			r2 := strings.Split(a, ":")
			r.name = r2[0]
			r2[1] = strings.TrimLeft(r2[1], "$@Role('")
			r2[1] = strings.TrimRight(r2[1], "')")
			r.role, _ = strconv.Atoi(r2[1])
			roles = append(roles, r)
		}
		m.roles = roles
	}
	return m, nil
}

// If present, the data attributes record of the IBM SPSS Statistics file.
type extendedNumberOfCases struct {
	// Record type. Always set to 7.
	recType int32

	// Record subtype. Always set to 16.
	subtype int32

	// Size of each element. Always set to 8.
	size int32

	// Number of pieces of data in the data part. Alway set to 2.
	count int32

	// Meaning unknown. Always set to 1.
	unknown int64

	// Number of cases in the file as a 64-bit integer. Presumably this
	// could be -1 to indicate that the number of cases is unknown, for the
	// same reason as ncases in the file header record.
	ncases int64
}

// readExtendedNumberOfCases returns a pointer to a extendednumberofcases and an error.
func (r *Reader) readExtendedNumberOfCases() (*extendedNumberOfCases, error) {
	m := new(extendedNumberOfCases)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.subtype, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.size, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.count, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.unknown, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.ncases, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Dictionary Termination.
type dictionaryTermination struct {
	recType int32
	filler  int32
}

// readDictionaryTermination returns a pointer to a dictionarytermination and an error.
func (r *Reader) readDictionaryTermination() (*dictionaryTermination, error) {
	m := new(dictionaryTermination)
	m.recType, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.filler, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Variable struct is a collection of useful information from the native spss structs.
type Variable struct {
	// n is the variable record index. Used as a helper to extract childVariables.
	n int

	// id is the 8 byte char from the variable record.
	id string

	// Name is the variable name from the variable record or if long from the verylongstring.
	Name string

	// Label is the variable label.
	Label string

	// Decimal is the number of decimals in the variable.
	Decimal int

	// Width is the number of bytes used for the variable.
	Width int

	// Numeric is a bool if true the variable is a numeric variable.
	Numeric bool

	// Type is the variable type.
	Type int

	// List of missing value and long missing value strings.
	MissingValues []interface{}

	// List of value labels from variable record and LongValueLabels.
	ValueLabels []*ValueLabel

	// The measurement of the variable, (1) Nominal, (2) Ordinal or (3) Continuous.
	Measure int

	// Helper to conclude how many child variables a variable has.
	childVariables int

	// chunks keeps track of how many variable record chunks it requires to build variable
	chunks int
}

// ValueLabel ...
type ValueLabel struct {
	Key   interface{}
	Value string
}

// Construct Variables from an existing spss struct.
func (r *Reader) constrVariables(h *Header) []*Variable {
	var variables []*Variable

	for i, vr := range h.Variable {
		v := new(Variable)
		if vr.tpe >= 0 {
			v.n = i
			v.id = vr.char
			v.Name = vr.char
			if h.LongVariableNames != nil {
				for _, longName := range h.LongVariableNames.varNamePairs {
					if strings.ToLower(vr.char) == strings.ToLower(longName.key) {
						v.Name = longName.value
					}
				}
			}
			if vr.hasVarLabel > 0 {
				v.Label = vr.label
			}
			v.Decimal = int(vr.print.decimal)
			v.Width = int(vr.print.width)
			if r.header != nil {
				v.Measure = int(r.header.VariableDisplay.display[i].measure)
			}
			if vr.tpe == 0 {
				v.Numeric = true
			}
			v.Type = int(vr.print.tpe)
			if vr.nMissingValues > 0 {
				v.MissingValues = append(v.MissingValues, vr.missingValues)
			}
			if h.LongStringMissingValues != nil {
				for _, longMissing := range h.LongStringMissingValues.missings {
					if strings.ToLower(vr.char) == strings.ToLower(longMissing.varName) {
						v.MissingValues = append(v.MissingValues, longMissing.missingValues)
					}
				}
			}
			if h.ValueLabel != nil {
				for _, valLabel := range h.ValueLabel {
					for _, chk := range valLabel.vlvr.vars {
						if v.n == int(chk) {
							for _, lbs := range valLabel.labels {
								vl := new(ValueLabel)
								vl.Key = strconv.FormatFloat(lbs.value, 'f', int(vr.print.decimal), 64)
								vl.Value = lbs.label
								v.ValueLabels = append(v.ValueLabels, vl)
							}
						}
					}
				}
			}
			if h.LongStringValueLabels != nil {
				for _, longValueLabel := range h.LongStringValueLabels.valueLabelPairs {
					if strings.ToLower(vr.char) == strings.ToLower(longValueLabel.varName) {
						for _, lbs := range longValueLabel.longLabels {
							vl := new(ValueLabel)
							vl.Key = lbs.value
							vl.Value = lbs.label
						}
					}
				}
			}
			variables = append(variables, v)
		} else {
			if variables != nil && len(variables) > 0 {
				//variables[len(variables)-1].Width += int(vr.print.width)
			}
		}
	}

	type subVariable struct {
		index  int
		parent int
		width  int
	}

	//Clean out superflouos data columns
	findParent := func(variables []*Variable) []*Variable {
		// Get the real variables from the role list.
		var realVariables []int
		for _, role := range h.VariableAttributes.roles {
			for _, v := range variables {
				if strings.ToLower(role.name) == strings.ToLower(v.Name) {
					realVariables = append(realVariables, v.n)
				}
			}
		}
		//
		intIn := func(n int, set []int) bool {
			for _, element := range set {
				if n == element {
					return true
				}
			}
			return false
		}
		//
		var vs []*Variable
		var subVars []*subVariable
		for _, element := range variables {
			var v *Variable
			if intIn(element.n, realVariables) {
				v = element
				vs = append(vs, v)
				continue
			}
			compareIndex := 999999999
			closestIndex := 0
			for _, elm := range variables {
				if ((element.n - elm.n) < compareIndex) && ((element.n - elm.n) > 0) && intIn(elm.n, realVariables) {
					compareIndex = element.n - elm.n
					closestIndex = elm.n
				}
			}
			sub := &subVariable{
				index:  element.n,
				parent: closestIndex,
				width:  element.Width,
			}
			subVars = append(subVars, sub)
		}

		//
		for _, element := range vs {
			for _, subelement := range subVars {
				if element.n == subelement.parent {
					element.childVariables++
					element.Width += subelement.width
				}
			}
			element.Width -= element.childVariables * 3
		}
		return vs
	}
	if h.VariableAttributes != nil {
		variables = findParent(variables)
	}
	return variables
}

// readDataRecord takes an existing Spss struct and returns a
// list of list of data and an error.
func (r *Reader) readDataRecord() (Row, error) {

	// buf is the chunk of 8 bytes to initiate each read with, bufindex is the current index.
	// cases is the list of list with cases.
	// var cases []Data
	var buf []byte
	bufindex := 8
	var chunksToRead int
	var charsToRead int

	for {
		var row Row
		for _, Var := range r.header.metaData {
			// Define a few helper variables
			var numData float64
			var strData string

			if Var.Numeric {
				chunksToRead = 1
			} else {
				charsToRead = Var.Width
				chunksToRead = int(math.Floor(float64(charsToRead-1)/8.0 + 1.0))
			}

			for chunksToRead > 0 {
				switch r.header.Fileheader.compression {
				case 0:
					// Add uncompressed support
					if Var.Numeric {
						numData, err = r.readFlt64()
						if err == io.EOF {
							return row, io.EOF
						}
						if err != nil {
							return nil, err
						}
						if numData == r.header.MachineFloatingPoint.sysmis {
							numData = math.NaN()
						}
					} else {
						txt, err := r.readString(8)
						if err == io.EOF {
							return row, io.EOF
						}
						if err != nil {
							return nil, err
						}
						strData = txt
						charsToRead -= 8
					}
				case 1, 2:
					// Byte compressed data.
					if bufindex > 7 {
						r.r.Peek(2000)
						buf, err = r.readBytes(8)
						if err == io.EOF {
							return row, io.EOF
						}
						if err != nil {
							return nil, err
						}
						bufindex = 0
					}
					// The current byte value we are evaluating.
					byteValue := int(buf[bufindex])
					bufindex++

					switch byteValue {
					// 0: Should be ignored.
					// 252: End of file.
					// 253: Compressed value.
					// 254: String filler.
					// 255: Missing value.
					case 0:
						continue
					case 252:
						return row, nil
					case 253:
						if Var.Numeric {
							numData, err = r.readFlt64()
							if err == io.EOF {
								return row, io.EOF
							}
							if err != nil {
								return nil, err
							}
						} else {
							chunkStringLen := int(math.Min(8.0, float64(charsToRead)))
							t, err := r.readString(chunkStringLen)
							if err == io.EOF {
								return row, io.EOF
							}
							if err != nil {
								return nil, err
							}
							strData += t

							if charsToRead < 8 {
								r.r.Discard(8 - charsToRead)
							}

							charsToRead -= chunkStringLen
						}
					case 254:
						strData += ""
					case 255:
						numData = math.NaN()
					default:
						numData = float64(byteValue - int(r.header.Fileheader.bias))
					}
				default:
					// Add error handling here
					return nil, io.EOF
				}
				chunksToRead--
			}
			if Var.Numeric {
				row = append(row, numData)
			} else {
				row = append(row, strings.TrimSpace(strData))
			}
		}
		if len(row) == 0 {
			return nil, io.EOF
		}
		return row, nil
	}
}

// If data record is zlib compressed.
type zLibDataHeader struct {
	zHeaderOffset  int64
	zTrailerOffset int64
	zTrailerLength int64
}

func (r *Reader) readZLibHeader() (*zLibDataHeader, error) {
	m := new(zLibDataHeader)
	r.r.Peek(2000)
	m.zHeaderOffset, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.zTrailerOffset, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.zTrailerLength, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// If data record is zlib compressed.
type zLibDataTrailer struct {
	bias               int64
	zero               int64
	blockSize          int32
	nBlocks            int32
	unCompressedOffset int64
	compressedOffset   int64
	unCompressedSize   int32
	compressedSize     int32
}

func (r *Reader) readZLibTrailer() (*zLibDataTrailer, error) {
	m := new(zLibDataTrailer)
	r.r.Peek(2000)
	m.bias, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.zero, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.blockSize, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.nBlocks, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.unCompressedOffset, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.compressedOffset, err = r.readInt64()
	if err != nil {
		return nil, err
	}
	m.unCompressedSize, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	m.compressedSize, err = r.readInt32()
	if err != nil {
		return nil, err
	}
	return m, nil
}

// Checks the system default endianess to infer the most likely endianess.
func machineEndianess() binary.ByteOrder {
	buf := [2]byte{}
	*(*uint16)(unsafe.Pointer(&buf[0])) = uint16(0xABCD)

	switch buf {
	case [2]byte{0xCD, 0xAB}:
		return binary.LittleEndian
	case [2]byte{0xAB, 0xCD}:
		return binary.BigEndian
	default:
		return nil
	}
}
