package Progetto

case class Event(
actor: Actor,
created_at: String,
id: Int,
org: String ,
payload: Payload,
public: Boolean,
repo: String,
_type: String

)
