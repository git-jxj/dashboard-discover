package main

import (
	"fmt"
	"net/http"
	"os"
	"path"
	"path/filepath"

	"encoding/hex"
	"encoding/json"
	"github.com/zeebo/xxh3"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	k8runtime "sigs.k8s.io/controller-runtime"
)

func InitLog(logLevel string) error {
	var lvl zap.AtomicLevel
	err := lvl.UnmarshalText([]byte(logLevel))
	if err != nil {
		fmt.Println(fmt.Sprintf("invalid log level: %s, fallback to info level", logLevel))
		lvl = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	}

	config := zap.NewProductionConfig()
	config.Level = zap.NewAtomicLevelAt(lvl.Level())
	config.OutputPaths = []string{"stdout"}
	config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	config.DisableStacktrace = true
	log, err := config.Build()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(log)

	return nil
}

func GetRestConfig(kubeConfig string) (*rest.Config, error) {
	var restCfg *rest.Config
	var err error

	if kubeConfig != "" {
		if restCfg, err = clientcmd.BuildConfigFromFlags("", kubeConfig); err != nil {
			return nil, fmt.Errorf("failed to get clientConfig from: %s, %v", err, kubeConfig)
		}
	} else {
		restCfg, err = k8runtime.GetConfig()
		if err != nil {
			return nil, fmt.Errorf("failed to get clientConfig: %v", err)
		}
	}

	return restCfg, nil
}

func ValidJson(ds []byte) bool {
	return json.Valid(ds)
}

func Hash(ds string) string {
	x3 := xxh3.New()
	x3.WriteString(ds)
	return hex.EncodeToString(x3.Sum(nil))
}

func GetNamespace(namespace string, labels map[string]string) string {
	if cfg.LabelAsSubFolder != "" {
		for k, v := range labels {
			if k == cfg.LabelAsSubFolder {
				return v
			}
		}
	}

	return namespace
}

func BuildFileName(name, namespace, resource string) string {
	return fmt.Sprintf("%s_%s_%s.%s", name, namespace, resource, "json")
}

func ReadFile(path string) (string, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(b), nil
}

func SaveFile(filePath, data string) error {
	if err := os.MkdirAll(filepath.Dir(filePath), 0755); err != nil {
		return fmt.Errorf("failed to create parent directory %q: %w", filePath, err)
	}

	err := os.WriteFile(filePath, []byte(data), 0644)
	if err != nil {
		return err
	}

	return nil
}

func DeleteEmptyDir(dirPath string) {
	fileInfo, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return
		}
		return
	}
	if !fileInfo.IsDir() {
		return
	}

	entries, err := os.ReadDir(dirPath)
	if err != nil {
		return
	}

	if len(entries) != 0 {
		return
	}

	err = os.Remove(dirPath)
	if err != nil {
		zap.S().Error(err)
	}

	return
}

func DeleteFile(fileName, subPath string) error {
	filePath := BuildPath(fileName, subPath)
	err := os.Remove(filePath)
	if err != nil {
		return err
	}

	DeleteEmptyDir(filepath.Dir(filePath))
	return nil
}

func BuildPath(fileName, subPath string) string {
	p := path.Join(cfg.Folder)
	if cfg.NamespaceAsSubFolder {
		p = path.Join(p, subPath)
	}

	p = path.Join(p, fileName)

	return p
}

func UpsertFile(fileName, subPath, data string) bool {
	if !ValidJson([]byte(data)) {
		zap.S().Errorf("invalid json format")
		return false
	}

	p := BuildPath(fileName, subPath)
	localData, err := ReadFile(p)
	if !os.IsNotExist(err) {
		if Hash(localData) == Hash(data) {
			return false
		}
	}

	err = SaveFile(p, data)
	if err != nil {
		zap.S().Error(err)
		return false
	}

	return true
}

func TriggerReload() {
	client := &http.Client{}
	req, err := http.NewRequest("POST", cfg.RequestReloadURL, nil)
	if err != nil {
		zap.S().Error(err)
		return
	}

	req.SetBasicAuth(cfg.RequestUser, cfg.RequestPassword)
	resp, err := client.Do(req)
	if err != nil {
		zap.S().Errorf("failed to trigger reload request: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		zap.S().Errorf("failed to trigger reload request: %v", resp.Status)
		return
	}

	zap.S().Infof("reload triggered")
	return
}
