package target

import "fmt"

type Stdout struct {
}

func (Stdout) String(in string) error {
	fmt.Println(in)
	return nil
}
