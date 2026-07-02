package types

type Read1Request struct {
	Field string
}

func (r *Read1Request) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read1Request) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

func (r *Read1Request) ShardKey() []byte {
	return []byte(r.Field)[:4]
}

type Read1Response struct {
	Field string
}

func (r *Read1Response) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read1Response) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Read2Request struct {
	Field string
}

func (r *Read2Request) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read2Request) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Read2Response struct {
	Field string
}

func (r *Read2Response) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read2Response) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Read3Request struct {
	Field string
}

func (r *Read3Request) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read3Request) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

func (r *Read3Request) ShardKey() []byte {
	return []byte(r.Field)[:4]
}

type Read3Response struct {
	Field string
}

func (r *Read3Response) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Read3Response) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Update1Request struct {
	Field string
}

func (r *Update1Request) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Update1Request) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

func (r *Update1Request) ShardKey() []byte {
	return []byte(r.Field)[:4]
}

type Update1Response struct {
	Field string
}

func (r *Update1Response) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Update1Response) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Update2Request struct {
	Field string
}

func (r *Update2Request) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Update2Request) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}

type Update2Response struct {
	Field string
}

func (r *Update2Response) MarshalBinary() ([]byte, error) {
	return []byte(r.Field), nil
}

func (r *Update2Response) UnmarshalBinary(data []byte) error {
	r.Field = string(data)
	return nil
}
