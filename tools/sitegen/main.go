// Command sitegen writes the site's documentation from the Markdown docs, so
// the site and the docs on GitHub say the same thing. It writes each docs
// page, the docs overview, pages that send the old addresses to the new ones,
// and the sitemap.
//
//	go -C tools/sitegen run . -root ../..          # write the site
//	go -C tools/sitegen run . -root ../.. -check   # fail if a file is out of date
//
// It is a module of its own, so the database module does not depend on it.
package main

import (
	"bytes"
	_ "embed"
	"flag"
	"fmt"
	"html/template"
	"log"
	"os"
	"path"
	"regexp"
	"strings"
	"unicode"

	"github.com/yuin/goldmark"
	"github.com/yuin/goldmark/ast"
	"github.com/yuin/goldmark/extension"
	"github.com/yuin/goldmark/renderer/html"
	"github.com/yuin/goldmark/text"
)

//go:embed page.html
var pageHTML string

//go:embed overview.html
var overviewHTML string

const (
	site   = "https://isledb.com"
	github = "https://github.com/ankur-anand/isledb/blob/main/"
)

// part is one source of a page: a Markdown file, or an HTML file for a page
// written as HTML. Its title is shown above it when a page has several
// parts, and its prefix keeps its anchors apart from the other parts'.
type part struct {
	file, title, prefix string
}

type page struct {
	path    string // the page's address, such as /docs/writer/
	group   string // its group in the sidebar
	nav     string // its name in the sidebar
	summary string // one line for the docs overview
	title, heading, description, lede string
	parts                             []part
	// tocDepth is the deepest heading level listed under "On this page".
	tocDepth int
}

var groups = []string{"Start", "Build", "Operate", "Reference"}

var pages = []page{
	{
		path: "/getting-started/", group: "Start", nav: "Get started",
		summary:     "Run a writer and a reader with Docker, or embed IsleDB in Go, in a few minutes.",
		title:       "Get Started with IsleDB: Key-Value Database on S3, over HTTP or in Go",
		heading:     "Get started with IsleDB",
		description: "Run IsleDB on your machine in a few minutes: start a writer and a reader with Docker, write and read keys over HTTP, or embed the Go library in your program.",
		lede:        "IsleDB keeps a key-value database in your object storage bucket. Run it as HTTP services and use it from any language, or embed it in your Go program.",
		parts:       []part{{file: "tools/sitegen/content/getting-started.html"}},
		tocDepth:    3,
	},
	{
		path: "/docs/http-api/", group: "Build", nav: "HTTP API",
		summary:     "Write and delete keys on the writer; read, scan, and follow the change feed on readers.",
		title:       "IsleDB HTTP API Reference: Writes, Reads, Scans, and the Change Feed",
		heading:     "HTTP API reference",
		description: "The IsleDB HTTP API: write and delete keys on the writer service, and read, scan, and follow the change feed on reader services.",
		lede:        "The writer service takes writes; reader services serve reads, scans, and the change feed. Both speak JSON over HTTP, from any language.",
		parts: []part{
			{file: "docs/writer-api.md", title: "Writer API", prefix: "writer-"},
			{file: "docs/reader-api.md", title: "Reader API", prefix: "reader-"},
		},
		tocDepth: 3,
	},
	{
		path: "/docs/go-api/", group: "Build", nav: "Go API",
		summary:     "Open a database, write and read keys, follow the change feed, and run maintenance from Go.",
		title:       "IsleDB Go API Reference: Key-Value Database for S3, GCS & Azure Blob",
		heading:     "Go API reference",
		description: "The IsleDB Go API: open a database on S3, GCS, or Azure Blob, write and read keys, follow the change feed, and run maintenance.",
		lede:        "Embed IsleDB in a Go program: open a database on your bucket, write and read keys, follow the change feed, and run maintenance.",
		parts:       []part{{file: "api.md"}},
		tocDepth:    2,
	},
	{
		path: "/docs/object-storage/", group: "Operate", nav: "Object storage",
		summary:     "Which object stores work, and how to create and secure a bucket for IsleDB.",
		title:       "Set Up Object Storage for IsleDB: S3, GCS, Azure Blob, and MinIO",
		heading:     "Set up object storage",
		description: "Which object stores IsleDB works with, and how to create a bucket, set permissions, and connect IsleDB to S3, Google Cloud Storage, Azure Blob, or MinIO.",
		parts:       []part{{file: "docs/object-store-guide.md"}},
		tocDepth:    2,
	},
	{
		path: "/docs/writer/", group: "Operate", nav: "Writer service",
		summary:     "Run isledb writer: configuration, authentication, the change feed, and metrics.",
		title:       "Run the IsleDB Writer Service: Configuration, Authentication, Metrics",
		heading:     "Writer service",
		description: "Run isledb writer, the service that takes writes for one IsleDB database over HTTP: configuration, authentication, the change feed, and metrics.",
		parts:       []part{{file: "docs/isledb-writer.md"}},
		tocDepth:    2,
	},
	{
		path: "/docs/reader/", group: "Operate", nav: "Reader service",
		summary:     "Run isledb reader: replicas, freshness, caches, and metrics.",
		title:       "Run the IsleDB Reader Service: Replicas, Freshness, Caches, Metrics",
		heading:     "Reader service",
		description: "Run isledb reader, the service that serves reads, scans, and the change feed of one IsleDB database over HTTP: replicas, freshness, caches, and metrics.",
		parts:       []part{{file: "docs/isledb-reader.md"}},
		tocDepth:    2,
	},
	{
		path: "/docs/maintenance/", group: "Operate", nav: "Maintenance service",
		summary:     "Run isledb maintenance: compaction, change-feed retention, and cleanup.",
		title:       "Run IsleDB Maintenance: Compaction, Retention, and Cleanup",
		heading:     "Maintenance service",
		description: "Run isledb maintenance, the service that compacts one IsleDB database, applies change-feed retention, and deletes what the database no longer needs.",
		parts:       []part{{file: "docs/isledb-maintenance.md"}},
		tocDepth:    2,
	},
	{
		path: "/docs/cache/", group: "Operate", nav: "Read cache",
		summary:     "Run isledb cache, the regional read cache, and the HTTP API readers use with it.",
		title:       "Run the IsleDB Read Cache: Serve Many Readers from One Copy",
		heading:     "Read cache",
		description: "Run isledb cache, the regional read cache that serves many IsleDB readers from one copy of each object, and the HTTP API readers use with it.",
		parts: []part{
			{file: "docs/isledb-cache.md", title: "Run the cache", prefix: "run-"},
			{file: "docs/read-cache-api.md", title: "Cache HTTP API", prefix: "api-"},
		},
		tocDepth: 3,
	},
	{
		path: "/docs/storage-layout/", group: "Reference", nav: "Storage layout",
		summary:     "What IsleDB keeps under a database's prefix, for backups, permissions, and recovery.",
		title:       "IsleDB Storage Layout: What Is in the Bucket",
		heading:     "Storage layout",
		description: "The objects IsleDB keeps under a database's prefix, and what operators need to know about them for backups, permissions, storage use, retention, and recovery.",
		parts:       []part{{file: "docs/object-store-schema.md"}},
		tocDepth:    2,
	},
}

// redirects send the addresses GitHub Pages used to publish the Markdown
// docs at to the pages that replace them.
var redirects = map[string]string{
	"reader-api.html":          "/docs/http-api/#reader",
	"writer-api.html":          "/docs/http-api/#writer",
	"isledb-writer.html":       "/docs/writer/",
	"isledb-reader.html":       "/docs/reader/",
	"isledb-maintenance.html":  "/docs/maintenance/",
	"isledb-cache.html":        "/docs/cache/",
	"read-cache-api.html":      "/docs/cache/#api",
	"object-store-guide.html":  "/docs/object-storage/",
	"object-store-schema.html": "/docs/storage-layout/",
}

func main() {
	check := flag.Bool("check", false, "fail if a file differs from what would be written")
	root := flag.String("root", ".", "the repository root")
	flag.Parse()
	if err := os.Chdir(*root); err != nil {
		log.Fatal(err)
	}
	files, err := build()
	if err != nil {
		log.Fatal(err)
	}
	stale := 0
	for _, f := range files {
		if *check {
			if old, err := os.ReadFile(f.name); err != nil || !bytes.Equal(old, f.data) {
				fmt.Fprintf(os.Stderr, "%s is out of date: run go -C tools/sitegen run . -root ../..\n", f.name)
				stale++
			}
			continue
		}
		if err := os.MkdirAll(path.Dir(f.name), 0o755); err != nil {
			log.Fatal(err)
		}
		if err := os.WriteFile(f.name, f.data, 0o644); err != nil {
			log.Fatal(err)
		}
	}
	if stale > 0 {
		os.Exit(1)
	}
}

type file struct {
	name string
	data []byte
}

// out is where a page at the address p is written.
func out(p string) string {
	return path.Join("docs", p, "index.html")
}

func build() ([]file, error) {
	var files []file
	for _, p := range pages {
		data, err := render(p)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", p.path, err)
		}
		files = append(files, file{out(p.path), data})
	}
	data, err := overview()
	if err != nil {
		return nil, err
	}
	files = append(files, file{out("/docs/"), data})
	for old, to := range redirects {
		files = append(files, file{path.Join("docs", old), redirect(to)})
	}
	files = append(files, file{"docs/sitemap.xml", sitemap()})
	return files, nil
}

type tocEntry struct {
	Level int
	ID    string
	Text  string
}

type navGroup struct {
	Name  string
	Pages []navPage
}

type navPage struct {
	Path, Name, Summary string
	Current             bool
}

func nav(current string) []navGroup {
	var out []navGroup
	for _, g := range groups {
		ng := navGroup{Name: g}
		for _, p := range pages {
			if p.group == g {
				ng.Pages = append(ng.Pages, navPage{Path: p.path, Name: p.nav, Summary: p.summary, Current: p.path == current})
			}
		}
		out = append(out, ng)
	}
	return out
}

func render(p page) ([]byte, error) {
	md := goldmark.New(
		goldmark.WithExtensions(extension.GFM),
		goldmark.WithRendererOptions(html.WithXHTML()),
	)
	var body bytes.Buffer
	var toc []tocEntry
	for _, pt := range p.parts {
		src, err := os.ReadFile(pt.file)
		if err != nil {
			return nil, err
		}
		if pt.title != "" {
			id := strings.TrimSuffix(pt.prefix, "-")
			fmt.Fprintf(&body, "<h2 class=\"part\" id=\"%s\">%s</h2>\n", id, template.HTMLEscapeString(pt.title))
			toc = append(toc, tocEntry{Level: 1, ID: id, Text: pt.title})
		}
		if strings.HasSuffix(pt.file, ".html") {
			body.Write(src)
			toc = append(toc, htmlHeadings(src, p.tocDepth)...)
			continue
		}
		src = dropContents(src)
		doc := md.Parser().Parse(text.NewReader(src))
		entries, err := prepare(doc, src, p, pt)
		if err != nil {
			return nil, err
		}
		toc = append(toc, entries...)
		if err := md.Renderer().Render(&body, src, doc); err != nil {
			return nil, err
		}
	}
	return execute(pageHTML, map[string]any{
		"Title":       p.title,
		"Heading":     p.heading,
		"Description": p.description,
		"Lede":        p.lede,
		"URL":         site + p.path,
		"Nav":         nav(p.path),
		"TOC":         toc,
		"Body":        template.HTML(body.String()),
	})
}

func overview() ([]byte, error) {
	return execute(overviewHTML, map[string]any{
		"URL": site + "/docs/",
		"Nav": nav("/docs/"),
	})
}

func execute(tpl string, data any) ([]byte, error) {
	t, err := template.New("page").Parse(tpl)
	if err != nil {
		return nil, err
	}
	var out bytes.Buffer
	err = t.Execute(&out, data)
	return out.Bytes(), err
}

// redirect is a page that sends a browser, and search engines, to to.
func redirect(to string) []byte {
	u := site + to
	return []byte(fmt.Sprintf(`<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>Moved</title>
    <link rel="canonical" href="%[1]s" />
    <meta http-equiv="refresh" content="0; url=%[1]s" />
  </head>
  <body>
    <p>This page has moved to <a href="%[1]s">%[1]s</a>.</p>
  </body>
</html>
`, u))
}

func sitemap() []byte {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	for _, p := range append([]string{"/", "/docs/"}, paths()...) {
		fmt.Fprintf(&b, "  <url>\n    <loc>%s%s</loc>\n  </url>\n", site, p)
	}
	b.WriteString("</urlset>\n")
	return []byte(b.String())
}

func paths() []string {
	var out []string
	for _, p := range pages {
		out = append(out, p.path)
	}
	return out
}

var headingRE = regexp.MustCompile(`<h([2-6]) id="([^"]+)"[^>]*>(.*?)</h[2-6]>`)
var tagRE = regexp.MustCompile(`<[^>]+>`)

// htmlHeadings lists the headings of a page written as HTML.
func htmlHeadings(src []byte, depth int) []tocEntry {
	var toc []tocEntry
	for _, m := range headingRE.FindAllSubmatch(src, -1) {
		level := int(m[1][0] - '0')
		if level <= depth {
			toc = append(toc, tocEntry{Level: level, ID: string(m[2]), Text: tagRE.ReplaceAllString(string(m[3]), "")})
		}
	}
	return toc
}

// dropContents removes a "## Contents" section: the page lists its own.
func dropContents(src []byte) []byte {
	re := regexp.MustCompile(`(?ms)^## Contents\n.*?(^## )`)
	return re.ReplaceAll(src, []byte("$1"))
}

// prepare edits a parsed document for the page: its title heading goes (the
// page has its own), the rest move down a level when the page has several
// parts, every heading gets an anchor, and links are rewritten to work on the
// site. It returns the headings to list under "On this page".
func prepare(doc ast.Node, src []byte, p page, pt part) ([]tocEntry, error) {
	shift := 0
	if pt.title != "" {
		shift = 1
	}
	seen := map[string]int{}
	var toc []tocEntry
	var drop []ast.Node
	err := ast.Walk(doc, func(n ast.Node, entering bool) (ast.WalkStatus, error) {
		if !entering {
			return ast.WalkContinue, nil
		}
		switch n := n.(type) {
		case *ast.Heading:
			if n.Level == 1 {
				drop = append(drop, n)
				return ast.WalkSkipChildren, nil
			}
			label := string(n.Text(src))
			base := slug(label)
			id := base
			if seen[base] > 0 {
				id = fmt.Sprintf("%s-%d", base, seen[base])
			}
			seen[base]++
			id = pt.prefix + id
			n.SetAttributeString("id", []byte(id))
			n.Level = min(n.Level+shift, 6)
			if n.Level <= p.tocDepth {
				toc = append(toc, tocEntry{Level: n.Level, ID: id, Text: label})
			}
		case *ast.Link:
			n.Destination = []byte(link(string(n.Destination), pt))
		}
		return ast.WalkContinue, nil
	})
	for _, n := range drop {
		n.Parent().RemoveChild(n.Parent(), n)
	}
	return toc, err
}

// link maps a link in a Markdown file to one that works on the site: an
// anchor in the same file gets the part's prefix, a file published on the
// site goes to its page, and any other file goes to GitHub.
func link(dest string, pt part) string {
	if strings.Contains(dest, "://") || strings.HasPrefix(dest, "mailto:") {
		return dest
	}
	if strings.HasPrefix(dest, "#") {
		return "#" + pt.prefix + dest[1:]
	}
	file, anchor, _ := strings.Cut(dest, "#")
	repoPath := path.Clean(path.Join(path.Dir(pt.file), file))
	for _, p := range pages {
		for _, other := range p.parts {
			if other.file != repoPath {
				continue
			}
			switch {
			case anchor != "":
				return p.path + "#" + other.prefix + anchor
			case other.prefix != "":
				return p.path + "#" + strings.TrimSuffix(other.prefix, "-")
			default:
				return p.path
			}
		}
	}
	if anchor != "" {
		return github + repoPath + "#" + anchor
	}
	return github + repoPath
}

// slug makes a heading's anchor the way GitHub does, so links written for the
// docs on GitHub work on the site.
func slug(s string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(s) {
		switch {
		case unicode.IsLetter(r), unicode.IsDigit(r), r == '_', r == '-':
			b.WriteRune(r)
		case r == ' ':
			b.WriteRune('-')
		}
	}
	return b.String()
}
