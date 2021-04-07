package models

import "time"

type Item struct {
	CreatedAt time.Time `json:"created_at"`
	Text string `json:"text"`
	Tag string `json:"tag"`
}
