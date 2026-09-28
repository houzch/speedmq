package management

import (
	"io/fs"
	"net/http"
	"strings"
)

// staticHandler 提供内嵌管理 UI 的静态资源服务。
//
// 缓存策略按"内容是否带指纹"区分：
//   - assets/ 下的文件名带内容哈希，可以长期强缓存（immutable）；
//   - index.html 绝不能缓存，否则前端发版后用户会一直拿到旧的入口文件，
//     进而加载已经删掉的 assets（表现为整页白屏，且刷新无效）。
func staticHandler(fsys fs.FS) http.Handler {
	fileServer := http.FileServer(http.FS(fsys))
	index, indexErr := fs.ReadFile(fsys, "index.html")

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, "/")
		if name == "" {
			name = "index.html"
		}

		if _, err := fs.Stat(fsys, name); err != nil {
			// SPA 回退：未知路径一律交给 index.html（前端用 hash 路由，只需要这一条回退）
			if indexErr != nil {
				http.Error(w, "管理 UI 未构建：请先执行 `npm run build`（产物目录 web/dist）", http.StatusNotFound)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Header().Set("Cache-Control", "no-cache")
			_, _ = w.Write(index)
			return
		}

		if strings.HasPrefix(name, "assets/") {
			w.Header().Set("Cache-Control", "public, max-age=31536000, immutable")
		} else {
			w.Header().Set("Cache-Control", "no-cache")
		}
		fileServer.ServeHTTP(w, r)
	})
}

// serveStatic 处理非 /api 路径：把请求交给内嵌 UI。
func (s *Server) serveStatic(w http.ResponseWriter, r *http.Request) {
	if s.static == nil {
		writeError(w, http.StatusNotFound, "Object Not Found",
			"管理 UI 资源未嵌入；管理 API 仍然可用（路径以 /api 开头）")
		return
	}
	s.static.ServeHTTP(w, r)
}
