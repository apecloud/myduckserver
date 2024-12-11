package replica

import (
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
)

// ReplicaOptions holds the options for a replica server.
// https://dev.mysql.com/doc/refman/8.4/en/replication-options-replica.html
type ReplicaOptions struct {
	ReportHost     string
	ReportPort     int
	ReportUser     string
	ReportPassword string
}

func RegisterReplicaOptions(options *ReplicaOptions) {
	sql.SystemVariables.AddSystemVariables([]sql.SystemVariable{
		&sql.MysqlSystemVariable{
			Name:              "report_host",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType("report_host"),
			Default:           options.ReportHost,
		},
		&sql.MysqlSystemVariable{
			Name:              "report_port",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemIntType("report_port", 0, 65535, false),
			Default:           int64(options.ReportPort),
		},
		&sql.MysqlSystemVariable{
			Name:              "report_user",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType("report_user"),
			Default:           options.ReportUser,
		},
		&sql.MysqlSystemVariable{
			Name:              "report_password",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           false,
			SetVarHintApplies: false,
			Type:              types.NewSystemStringType("report_password"),
			Default:           options.ReportPassword,
		},
		// MyDuck-specific system variables
		&sql.MysqlSystemVariable{
			Name:              "replica_is_loading_snapshot",
			Scope:             sql.GetMysqlScope(sql.SystemVariableScope_Global),
			Dynamic:           true,
			SetVarHintApplies: false,
			Type:              types.NewSystemBoolType("replica_is_loading_snapshot"),
			Default:           false,
		},
	})
}
