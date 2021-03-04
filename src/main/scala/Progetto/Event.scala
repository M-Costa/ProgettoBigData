package Progetto

case class Event(
actor: Actor,
created_at: java.sql.Timestamp,
id: String,
org: String ,
payload: Payload,
publicFild: Boolean,
repo: String,
`type`: String

)
