// Code generated from JSON Schema using quicktype. DO NOT EDIT.
// To parse and unparse this JSON data, add this code to your project and do:
//
//    dealDispatchedData, err := UnmarshalDealDispatchedData(bytes)
//    bytes, err = dealDispatchedData.Marshal()

package events

import "time"

import "encoding/json"

func UnmarshalDealDispatchedData(data []byte) (DealDispatchedData, error) {
	var r DealDispatchedData
	err := json.Unmarshal(data, &r)
	return r, err
}

func (r *DealDispatchedData) Marshal() ([]byte, error) {
	return json.Marshal(r)
}

type DealDispatchedData struct {
	Client   Client   `json:"client"`
	Company  Company  `json:"company"`
	Deal     Deal     `json:"deal"`
	Instance Instance `json:"instance"`
	Manager  Manager  `json:"manager"`
}

type Client struct {
	// Client ID in system                                             
	ID                                                          int64  `json:"id"`
	// Client's phone number in E.164 digits-only format, no '+'       
	PhoneNumber                                                 string `json:"phone_number"`
}

type Company struct {
	// Company ID      
	ID           int64 `json:"id"`
}

type Deal struct {
	// Internal deal ID                         
	ID                                int64     `json:"id"`
	// Additional info                          
	Info                              string    `json:"info"`
	// Deal number relative to company          
	RelativeNumber                    int64     `json:"relative_number"`
	// When the deal was dispatched             
	RequestDate                       time.Time `json:"request_date"`
}

type Instance struct {
	// Instance ID      
	ID            int64 `json:"id"`
}

type Manager struct {
	// Manager ID                                                     
	ID                                                         int64  `json:"id"`
	// Manager's magic link                                           
	MagicLink                                                  string `json:"magic_link"`
	// Full name of assigned manager                                  
	Name                                                       string `json:"name"`
	// Manager phone number in E.164 digits-only format, no '+'       
	PhoneNumber                                                string `json:"phone_number"`
}
