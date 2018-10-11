package provider

type OptionConfig map[string]interface{}

type Provider interface {
	String() string
	Config() OptionConfig
	Open() error
	Close() error
	Write(data []byte) error
}
