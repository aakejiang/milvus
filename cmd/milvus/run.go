package milvus

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	RunCmd      = "run"
	roleMixture = "mixture"
)

type run struct {
	serverType string
	// flags
	svrAlias                                                             string
	enableRootCoord, enableQueryCoord, enableIndexCoord, enableDataCoord bool
}

func (c *run) getHelp() string {
	return runLine + "\n" + serverTypeLine
}

func (c *run) execute(args []string, flags *flag.FlagSet) {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, c.getHelp())
		return
	}
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, c.getHelp())
	}
	c.serverType = args[2]
	c.formatFlags(args, flags)

	var local = false
	role := roles.MilvusRoles{}
	switch c.serverType {
	case typeutil.RootCoordRole:
		role.EnableRootCoord = true
	case typeutil.ProxyRole:
		role.EnableProxy = true
	case typeutil.QueryCoordRole:
		role.EnableQueryCoord = true
	case typeutil.QueryNodeRole:
		role.EnableQueryNode = true
	case typeutil.DataCoordRole:
		role.EnableDataCoord = true
	case typeutil.DataNodeRole:
		role.EnableDataNode = true
	case typeutil.IndexCoordRole:
		role.EnableIndexCoord = true
	case typeutil.IndexNodeRole:
		role.EnableIndexNode = true
	case typeutil.StandaloneRole, typeutil.EmbeddedRole:
		role.HasMultipleRoles = true
		role.EnableRootCoord = true
		role.EnableProxy = true
		role.EnableQueryCoord = true
		role.EnableQueryNode = true
		role.EnableDataCoord = true
		role.EnableDataNode = true
		role.EnableIndexCoord = true
		role.EnableIndexNode = true
		local = true
	case roleMixture:
		role.HasMultipleRoles = true
		role.EnableRootCoord = c.enableRootCoord
		role.EnableQueryCoord = c.enableQueryCoord
		role.EnableDataCoord = c.enableDataCoord
		role.EnableIndexCoord = c.enableIndexCoord
	default:
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n%s", c.serverType, c.getHelp())
		os.Exit(-1)
	}

	// Setup logger in advance for standalone and embedded Milvus.
	// Any log from this point on is under control.
	if c.serverType == typeutil.StandaloneRole || c.serverType == typeutil.EmbeddedRole {
		var params paramtable.BaseTable
		if c.serverType == typeutil.EmbeddedRole {
			params.GlobalInitWithYaml("embedded-milvus.yaml")
		} else {
			params.Init()
		}
		params.SetLogConfig()
		params.RoleName = c.serverType
		params.SetLogger(0)
	}

	runtimeDir := createRuntimeDir(c.serverType)
	filename := getPidFileName(c.serverType, c.svrAlias)

	c.printBanner(flags.Output())
	c.injectVariablesToEnv()
	lock, err := createPidFile(flags.Output(), filename, runtimeDir)
	if err != nil {
		panic(err)
	}
	defer removePidFile(lock)
	role.Run(local, c.svrAlias)
}

func (c *run) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.svrAlias, "alias", "", "set alias")

	flags.BoolVar(&c.enableRootCoord, typeutil.RootCoordRole, false, "enable root coordinator")
	flags.BoolVar(&c.enableQueryCoord, typeutil.QueryCoordRole, false, "enable query coordinator")
	flags.BoolVar(&c.enableIndexCoord, typeutil.IndexCoordRole, false, "enable index coordinator")
	flags.BoolVar(&c.enableDataCoord, typeutil.DataCoordRole, false, "enable data coordinator")

	initMaxprocs(c.serverType, flags)
	if err := flags.Parse(args[3:]); err != nil {
		os.Exit(-1)
	}
}

func (c *run) printBanner(w io.Writer) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "    __  _________ _   ____  ______    ")
	fmt.Fprintln(w, "   /  |/  /  _/ /| | / / / / / __/    ")
	fmt.Fprintln(w, "  / /|_/ // // /_| |/ / /_/ /\\ \\    ")
	fmt.Fprintln(w, " /_/  /_/___/____/___/\\____/___/     ")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Welcome to use Milvus!")
	fmt.Fprintln(w, "Version:   "+BuildTags)
	fmt.Fprintln(w, "Built:     "+BuildTime)
	fmt.Fprintln(w, "GitCommit: "+GitCommit)
	fmt.Fprintln(w, "GoVersion: "+GoVersion)
	fmt.Fprintln(w)
}

func (c *run) injectVariablesToEnv() {
	// inject in need

	var err error

	err = os.Setenv(metricsinfo.GitCommitEnvKey, GitCommit)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.GitCommitEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.GitBuildTagsEnvKey, BuildTags)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.GitBuildTagsEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.MilvusBuildTimeEnvKey, BuildTime)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.MilvusBuildTimeEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.MilvusUsedGoVersion, GoVersion)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.MilvusUsedGoVersion),
			zap.Error(err))
	}
}
