package handler

import (
	"bytes"
	"cloud_distributed_storage/Backend/search"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/google/uuid"
	"github.com/ledongthuc/pdf"
	"github.com/otiai10/gosseract/v2"
	"github.com/tealeg/xlsx"
	"github.com/unidoc/unioffice/document"
	"github.com/youpy/go-wav"
)

type EmbeddingModel interface {
	EmbedText(text string) ([]float32, error)
}

type Processor struct {
	esClient  *elasticsearch.Client
	model     EmbeddingModel
	indexName string
}

func NewProcessor(esClient *elasticsearch.Client, model EmbeddingModel, indexName string) *Processor {
	return &Processor{
		esClient:  esClient,
		model:     model,
		indexName: indexName,
	}
}

func (p *Processor) ProcessFile(filePath string, reader io.Reader) error {
	ext := strings.ToLower(filepath.Ext(filePath))
	var text string
	var err error

	switch ext {
	case ".pdf":
		text, err = extractTextFromPDF(reader)
	case ".doc", ".docx":
		text, err = extractTextFromDoc(reader)
	case ".jpg", ".jpeg", ".png":
		text, err = extractTextFromImage(reader)
	case ".wav":
		text, err = extractTextFromAudio(reader)
	case ".xlsx", ".xls":
		text, err = extractTextFromExcel(reader)
	default:
		return fmt.Errorf("unsupported file type: %s", ext)
	}

	if err != nil {
		return err
	}

	vector, err := p.model.EmbedText(text)
	if err != nil {
		return err
	}

	id := uuid.New().String()
	err = search.InsertVectorData(p.esClient, p.indexName, id, vector, text)
	if err != nil {
		return err
	}

	return nil
}

func extractTextFromPDF(reader io.Reader) (string, error) {
	content, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	pdfReader, err := pdf.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return "", err
	}

	var text strings.Builder
	for i := 1; i <= pdfReader.NumPage(); i++ {
		page := pdfReader.Page(i)
		pageText, err := page.GetPlainText(nil)
		if err != nil {
			return "", err
		}
		text.WriteString(pageText)
	}

	return text.String(), nil
}

func extractTextFromDoc(reader io.Reader) (string, error) {
	doc, err := document.Read(reader)
	if err != nil {
		return "", err
	}

	var text strings.Builder
	for _, para := range doc.Paragraphs() {
		for _, run := range para.Runs() {
			text.WriteString(run.Text())
		}
		text.WriteString("\n")
	}

	return text.String(), nil
}

func extractTextFromImage(reader io.Reader) (string, error) {
	client := gosseract.NewClient()
	defer client.Close()

	content, err := io.ReadAll(reader)
	if err != nil {
		return "", err
	}

	err = client.SetImageFromBytes(content)
	if err != nil {
		return "", err
	}

	text, err := client.Text()
	if err != nil {
		return "", err
	}

	return text, nil
}

func extractTextFromAudio(reader io.Reader) (string, error) {
	// 注意：这里只是一个简单的示例，实际的语音识别需要更复杂的处理
	wavReader := wav.NewReader(reader)
	_, err := wavReader.Format()
	if err != nil {
		return "", err
	}

	// 这里应该使用实际的语音识别库
	return "Extracted text from audio", nil
}

func extractTextFromExcel(reader io.Reader) (string, error) {
	xlFile, err := xlsx.OpenReaderAt(reader, -1)
	if err != nil {
		return "", err
	}

	var text strings.Builder
	for _, sheet := range xlFile.Sheets {
		for _, row := range sheet.Rows {
			for _, cell := range row.Cells {
				text.WriteString(cell.String())
				text.WriteString("\t")
			}
			text.WriteString("\n")
		}
	}

	return text.String(), nil
}
