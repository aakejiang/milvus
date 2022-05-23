package table

type User struct {
	TenantID          string `db:"tenant_id"`
	Username          string `db:"username"`
	EncryptedPassword string `db:"encrypted_password"`
	IsSuper           bool   `db:"is_super"`
	IsDeleted         bool   `db:"is_deleted"`
}
