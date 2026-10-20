package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/grafana-operator/grafana-operator/v4/api/integreatly/v1alpha1"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"go.uber.org/zap"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	_ "k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

var (
	cfg     Config
	rootCmd = &cobra.Command{
		Use:   "dashboard-discover",
		Short: "Start dashboard-discover",
		RunE:  RunE,
	}
)

type Config struct {
	LogLevel                 string
	TelemetryAddress         string
	KubConfig                string
	ConfigMapSelector        string
	GrafanadashboardSelector string
	LabelAsSubFolder         string
	Folder                   string
	NamespaceAsSubFolder     bool
	EnableReload             bool
	RequestReloadURL         string
	RequestUser              string
	RequestPassword          string
}

func init() {
	cfg = Config{}
	rootCmd.PersistentFlags().StringVar(&cfg.LogLevel, "log-level", "info", "log level")
	rootCmd.PersistentFlags().StringVar(&cfg.TelemetryAddress, "telemetry-address", ":8082", "health server address")
	rootCmd.PersistentFlags().StringVar(&cfg.KubConfig, "kubeconfig", "", "path to the kubeconfig file, if not specified, default is $HOME/.kube/config or from env var KUBECONFIG")
	rootCmd.PersistentFlags().StringVar(&cfg.ConfigMapSelector, "configmap-selector", "dashboard=true,grafana=true", "Kubernetes label selector to filter which ConfigMaps to watch (format: key=value, supports multiple via comma separation)")
	rootCmd.PersistentFlags().StringVar(&cfg.GrafanadashboardSelector, "grafanadashboard-selector", "dashboard=true", "Kubernetes label selector to filter which grafanadashboards.integreatly.org to watch (format: key=value, supports multiple via comma separation)")
	rootCmd.PersistentFlags().StringVar(&cfg.LabelAsSubFolder, "label-as-subfolder", "folder", "save files into a subfolder named by the value of a specific resource label. Takes priority over the --namespace-as-subfolder flag if set")
	rootCmd.PersistentFlags().StringVar(&cfg.Folder, "folder", "./tmp", "folder to save file")
	rootCmd.PersistentFlags().BoolVar(&cfg.NamespaceAsSubFolder, "namespace-as-subfolder", true, "Save files into subfolders named after their namespace")
	rootCmd.PersistentFlags().BoolVar(&cfg.EnableReload, "enable-reload", false, "enable reload")
	rootCmd.PersistentFlags().StringVar(&cfg.RequestReloadURL, "request-reload-url", "http://localhost:3000/api/admin/provisioning/dashboards/reload", "URL to which send a request after a configmap got reloaded")
	rootCmd.PersistentFlags().StringVar(&cfg.RequestUser, "request-user", "", "user to whom the request is sent, or from `REQUEST_USER` env var")
	rootCmd.PersistentFlags().StringVar(&cfg.RequestPassword, "request-password", "", "password to whom the request is sent, or from `REQUEST_PASSWORD` env var")
	if cfg.RequestUser == "" {
		cfg.RequestUser = os.Getenv("REQUEST_USER")
	}

	if cfg.RequestPassword == "" {
		cfg.RequestPassword = os.Getenv("REQUEST_PASSWORD")
	}
}

func RunE(cmd *cobra.Command, _ []string) error {
	err := InitLog(cfg.LogLevel)
	if err != nil {
		return err
	}

	fmt.Println("--- All Flags ---")
	cmd.Flags().VisitAll(func(f *pflag.Flag) {
		fmt.Printf("%-20s = %v\n", f.Name, f.Value)
	})
	go StartDiscover()
	HealthServer()

	return nil
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func NewConfigMapDiscover(restCfg *rest.Config) (cache.Controller, error) {
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return nil, err
	}

	_, err = labels.Parse(cfg.ConfigMapSelector)
	if err != nil {
		return nil, fmt.Errorf("unable to parse %s: %w", cfg.ConfigMapSelector, err)
	}

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: &cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options v1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = cfg.ConfigMapSelector
				return client.CoreV1().ConfigMaps("").List(ctx, options)
			},
			WatchFuncWithContext: func(ctx context.Context, options v1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = cfg.ConfigMapSelector
				options.Watch = true
				return client.CoreV1().ConfigMaps("").Watch(ctx, options)
			},
		},
		ObjectType: &corev1.ConfigMap{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				cm := obj.(*corev1.ConfigMap)
				r := false
				for k, v := range cm.Data {
					ns := GetNamespace(cm.Namespace, cm.Labels)
					fileName := BuildFileName(k, ns, "cm")
					if UpsertFile(fileName, ns, v) {
						r = true
					}
				}
				if cfg.EnableReload && r {
					TriggerReload()
				}
				zap.S().Debugf("configMap source dashboard added: %s %s", cm.Name, cm.Namespace)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				cm := newObj.(*corev1.ConfigMap)
				r := false
				for k, v := range cm.Data {
					ns := GetNamespace(cm.Namespace, cm.Labels)
					fileName := BuildFileName(k, ns, "cm")
					if UpsertFile(fileName, ns, v) {
						r = true
					}
				}
				if cfg.EnableReload && r {
					TriggerReload()
				}
				zap.S().Debugf("configMap source dashboard updated: %s %s", cm.Name, cm.Namespace)
			},
			DeleteFunc: func(obj interface{}) {
				cm := obj.(*corev1.ConfigMap)
				count := 0
				for k := range cm.Data {
					ns := GetNamespace(cm.Namespace, cm.Labels)
					fileName := BuildFileName(k, ns, "cm")
					err := DeleteFile(fileName, ns)
					if err != nil {
						zap.S().Error(err)
						continue
					}

					count++
				}

				if cfg.EnableReload && count > 0 {
					TriggerReload()
				}
				zap.S().Debugf("configMap source dashboard deleted: %s %s", cm.Name, cm.Namespace)
			},
		},
		ResyncPeriod: 60 * time.Second,
	})

	return controller, nil
}

func NewGrafanaDashboardV4ClientV2(config *rest.Config) (rest.Interface, error) {
	config.ContentConfig.GroupVersion = &v1alpha1.GroupVersion
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()
	err := v1alpha1.AddToScheme(scheme.Scheme)
	if err != nil {
		return nil, err
	}

	client, err := rest.RESTClientFor(config)
	if err != nil {
		return nil, err
	}

	return client, nil
}

func NewGrafanaDashboardV4Discover(restCfg *rest.Config) (cache.Controller, error) {
	client, err := NewGrafanaDashboardV4ClientV2(restCfg)
	if err != nil {
		return nil, err
	}

	_, err = labels.Parse(cfg.GrafanadashboardSelector)
	if err != nil {
		return nil, fmt.Errorf("unable to parse %s: %w", cfg.GrafanadashboardSelector, err)
	}

	_, controller := cache.NewInformerWithOptions(cache.InformerOptions{
		ListerWatcher: &cache.ListWatch{
			ListWithContextFunc: func(ctx context.Context, options v1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = cfg.GrafanadashboardSelector
				result := &v1alpha1.GrafanaDashboardList{}
				err = client.Get().Resource("grafanadashboards").VersionedParams(&options, scheme.ParameterCodec).Do(ctx).Into(result)
				if err != nil {
					return nil, err
				}

				return result, nil
			},
			WatchFuncWithContext: func(ctx context.Context, options v1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = cfg.GrafanadashboardSelector
				options.Watch = true
				return client.Get().Resource("grafanadashboards").VersionedParams(&options, scheme.ParameterCodec).Watch(ctx)
			},
		},
		ObjectType: &v1alpha1.GrafanaDashboard{},
		Handler: cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ds := obj.(*v1alpha1.GrafanaDashboard)
				ns := GetNamespace(ds.Namespace, ds.Labels)
				fileName := BuildFileName(ds.Name, ns, "grafanadashboard(v4)")
				r := UpsertFile(fileName, ns, ds.Spec.Json)
				if cfg.EnableReload && r {
					TriggerReload()
				}
				zap.S().Debugf("grafanadashboard(v4) source dashboard added: %s %s", ds.Name, ds.Namespace)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				ds := newObj.(*v1alpha1.GrafanaDashboard)
				ns := GetNamespace(ds.Namespace, ds.Labels)
				fileName := BuildFileName(ds.Name, ns, "grafanadashboard(v4)")
				r := UpsertFile(fileName, ns, ds.Spec.Json)
				if cfg.EnableReload && r {
					TriggerReload()
				}
				zap.S().Debugf("grafanadashboard(v4) source dashboard updated: %s %s", ds.Name, ds.Namespace)
			},
			DeleteFunc: func(obj interface{}) {
				ds := obj.(*v1alpha1.GrafanaDashboard)
				ns := GetNamespace(ds.Namespace, ds.Labels)
				fileName := BuildFileName(ds.Name, ns, "grafanadashboard(v4)")
				err = DeleteFile(fileName, ns)
				if err != nil {
					zap.S().Error(err)
					return
				}

				if cfg.EnableReload {
					TriggerReload()
				}
				zap.S().Debugf("grafanadashboard(v4) source dashboard deleted: %s %s", ds.Name, ds.Namespace)
			},
		},
		ResyncPeriod: 60 * time.Second,
	})

	return controller, nil
}

func HealthServer() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ok")
	})
	http.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "ok")
	})

	zap.S().Infof("health server listening on %s", cfg.TelemetryAddress)
	zap.S().Fatal(http.ListenAndServe(cfg.TelemetryAddress, nil))
}

func StartDiscover() {
	restCfg, err := GetRestConfig(cfg.KubConfig)
	if err != nil {
		zap.S().Error(err)
		return
	}

	if cfg.ConfigMapSelector != "" {
		controller, err := NewConfigMapDiscover(restCfg)
		if err != nil {
			zap.S().Error(err)
		}

		go controller.RunWithContext(context.Background())
		zap.S().Infof("configmap discovery started")
	}

	if cfg.GrafanadashboardSelector != "" {
		discover, err := NewGrafanaDashboardV4Discover(restCfg)
		if err != nil {
			zap.S().Error(err)
		}

		zap.S().Infof("grafanadashboards(v4) discovery started")
		go discover.RunWithContext(context.Background())
	}
}
