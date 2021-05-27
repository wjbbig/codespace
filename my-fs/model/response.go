package model

type Response struct {
	Code int         `json:"code"`
	Data interface{} `json:"data"`
	Err  string      `json:"err"`
}

func NewSuccessResp(data interface{}) Response {
	return Response{
		Code: 200,
		Data: data,
	}
}

func NewErrorResp(err string) Response {
	return Response{
		Code: 400,
		Err:  err,
	}
}
