package nosql

func ConnectToBackend(backend_name string, options map[string]string) (*Backend, error) {

	var backend Backend

	if backend_name == "dynamodb" {
		backend = &BackendDynamoDB{}
		err := backend.Connect(options)
		if err != nil {
			return nil, err
		}
	} else {
		backend = &BackendMongoDB{}
		err := backend.Connect(options)
		if err != nil {
			return nil, err
		}
	}

	return &backend, nil
}
