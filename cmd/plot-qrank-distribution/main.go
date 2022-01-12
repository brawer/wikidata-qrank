// Tool for plotting the QRank distribution.
//
// SPDX-FileCopyrightText: 2022 Sascha Brawer <sascha@brawer.ch>
// SPDX-License-Identifier: MIT

package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/fogleman/gg"
)

func main() {
	font := flag.String("font", "./RobotoSlab-Light.ttf", "path to label font")
	qrank := flag.String("qrank", "qrank.csv.gz", "path to QRank file")
	out := flag.String("out", "qrank-distribution.png", "path to output file being written")
	flag.Parse()
	if err := PlotDistribution(*font, *qrank, *out); err != nil {
		log.Fatal(err)
	}
}

func PlotDistribution(fontPath, qrankPath, outPath string) error {
	axisWidth := 35.0
	plotWidth := 1000.0
	logX, logY := false, true
	dc := gg.NewContext(int(plotWidth+axisWidth), int(plotWidth+axisWidth))
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGB(0, 0, 0)

	font, err := gg.LoadFontFace(fontPath, 18.0)
	if err != nil {
		return err
	}

	smallFont, err := gg.LoadFontFace(fontPath, 11.0)
	if err != nil {
		return err
	}

	qrankFile, err := os.Open(qrankPath)
	if err != nil {
		return err
	}
	defer qrankFile.Close()

	qrankReader, err := gzip.NewReader(qrankFile)
	if err != nil {
		return err
	}

	numRanks, err := CountLines(qrankReader)
	if err != nil {
		return err
	}
	numRanks -= 1 // Don’t count CSV header.
	numRanksInMillions := int(numRanks / 1000000)

	scaleX := plotWidth / math.Ceil(math.Log(float64(numRanks)))
	if !logX {
		scaleX = plotWidth / (float64(numRanksInMillions+1) * 1e6)
	}

	if logX {
		for i := 0; i <= int(math.Log(float64(numRanks))); i++ {
			x := axisWidth + float64(i)*scaleX
			dc.MoveTo(x, plotWidth)
			dc.LineTo(x, plotWidth+5)
			dc.Stroke()
			dc.SetFontFace(font)
			eWidth, eHeight := dc.MeasureString("e")
			dc.DrawString("e", x-3, plotWidth+23)
			dc.SetFontFace(smallFont)
			dc.DrawString(strconv.Itoa(i), x-3+eWidth, plotWidth+23-eHeight/2)
		}
	} else {
		for i := 0; i <= numRanksInMillions; i += 2 {
			x := axisWidth + float64(i)*1e6*scaleX
			dc.MoveTo(x, plotWidth)
			dc.LineTo(x, plotWidth+5)
			dc.Stroke()
			dc.SetFontFace(font)
			dc.DrawString(strconv.Itoa(i)+"M", x-3, plotWidth+23)
		}
	}

	if _, err := qrankFile.Seek(0, os.SEEK_SET); err != nil {
		return err
	}

	dc.SetFontFace(font)
	w, _ := dc.MeasureString("Rank")
	dc.DrawString("Rank", axisWidth+(plotWidth-w)/2, plotWidth-12)

	qrankReader, err = gzip.NewReader(qrankFile)
	if err != nil {
		return err
	}

	var line int64 = 1
	scanner := bufio.NewScanner(qrankReader)
	scanner.Scan() // Skip CSV header.

	dc.SetRGB(0, 0.4, 1)
	var scaleY float64
	var maxValue float64
	var lastX, lastY float64

	type point struct{ x, y float64 }
	graph := make([]point, 0, int(plotWidth))
	for scanner.Scan() {
		line += 1
		cols := strings.Split(scanner.Text(), ",")
		if len(cols) < 2 {
			return fmt.Errorf("%s:%d: less than 2 columns", qrankPath, line)
		}
		val, err := strconv.ParseInt(cols[1], 10, 64)
		if err != nil {
			return err
		}

		if line == 2 { // first item in file
			maxValue = float64(val)
			if logY {
				scaleY = plotWidth / math.Ceil(math.Log10(maxValue))
			} else {
				scaleY = plotWidth / maxValue
			}
		}

		x := math.Log(float64(line-1))*scaleX + axisWidth
		if !logX {
			x = float64(line-1)*scaleX + axisWidth
		}
		y := plotWidth - math.Log10(float64(val))*scaleY
		if !logY {
			y = plotWidth - float64(val)*scaleY
		}

		if line == 2 || x-lastX > 1 || y-lastY > 1 {
			lastX, lastY = x, y
			graph = append(graph, point{x, y})
		}

		if false && line == 50000 {
			break
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	for i, p := range graph {
		if i == 0 {
			dc.MoveTo(p.x, p.y)
		} else {
			dc.LineTo(p.x, p.y)
		}
	}
	dc.Stroke()

	for _, p := range graph {
		dc.DrawCircle(p.x, p.y, 4)
		dc.Fill()
	}

	dc.SetRGB(0, 0, 0)
	dc.Push()
	dc.RotateAbout(-math.Pi/2, plotWidth/2, plotWidth/2)
	dc.DrawString("Views", plotWidth/2, axisWidth+24)
	dc.Pop()

	dc.MoveTo(axisWidth, plotWidth-math.Log10(maxValue)*scaleY)
	dc.LineTo(axisWidth, plotWidth)
	if logX {
		dc.LineTo(axisWidth+math.Log(float64(numRanks))*scaleX, plotWidth)
	} else {
		dc.LineTo(axisWidth+float64(numRanks)*scaleX, plotWidth)
	}
	dc.Stroke()

	if logY {
		for i := 0; i <= int(math.Log10(float64(maxValue))); i++ {
			y := plotWidth - float64(i)*scaleY
			dc.MoveTo(axisWidth-5, y)
			dc.LineTo(axisWidth, y)
			dc.Stroke()
			dc.SetFontFace(font)
			eWidth, eHeight := dc.MeasureString("10")
			dc.DrawString("10", 5, y)
			dc.SetFontFace(smallFont)
			dc.DrawString(strconv.Itoa(i), 5+eWidth, y-eHeight/2)
		}
	}

	if err := dc.SavePNG(outPath); err != nil {
		return err
	}

	return nil
}

func CountLines(r io.Reader) (int64, error) {
	var count int64
	buf := make([]byte, 64*1024)
	for {
		bufSize, err := r.Read(buf)
		if err != nil && err != io.EOF {
			return 0, err
		}
		var pos int
		for {
			i := bytes.IndexByte(buf[pos:], '\n')
			if i == -1 || pos == bufSize {
				break
			}
			pos += i + 1
			count += 1
		}
		if err == io.EOF {
			break
		}
	}
	return count, nil
}

func DrawSpiral() {
	const S = 1024
	const N = 2048
	dc := gg.NewContext(S, S)
	dc.SetRGB(1, 1, 1)
	dc.Clear()
	dc.SetRGB(0, 0, 0)
	for i := 0; i <= N; i++ {
		t := float64(i) / N
		d := t*S*0.4 + 10
		a := t * math.Pi * 2 * 20
		x := S/2 + math.Cos(a)*d
		y := S/2 + math.Sin(a)*d
		//r := 3.0 //t * 8
		//dc.DrawCircle(x, y, r)
		dc.SetPixel(int(x), int(y))
	}
	dc.Fill()
	dc.SavePNG("spiral.png")
}
