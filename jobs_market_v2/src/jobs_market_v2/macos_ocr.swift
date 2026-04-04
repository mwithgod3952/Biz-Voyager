import Foundation
import Vision
import AppKit
import PDFKit

func recognize(_ image: CGImage) throws {
    let request = VNRecognizeTextRequest()
    request.recognitionLevel = .accurate
    request.usesLanguageCorrection = true
    request.recognitionLanguages = ["ko-KR", "en-US"]
    let handler = VNImageRequestHandler(cgImage: image, options: [:])
    try handler.perform([request])
    let lines = (request.results ?? []).compactMap { $0.topCandidates(1).first?.string }
    print(lines.joined(separator: "\n"))
}

let path = CommandLine.arguments[1]
let url = URL(fileURLWithPath: path)

if path.lowercased().hasSuffix(".pdf") {
    guard let document = PDFDocument(url: url), let page = document.page(at: 0) else {
        fputs("failed to open pdf\n", stderr)
        exit(1)
    }
    if let text = page.string, !text.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
        print(text)
        exit(0)
    }
    let bounds = page.bounds(for: .mediaBox)
    let scale: CGFloat = 2.0
    let width = Int(bounds.width * scale)
    let height = Int(bounds.height * scale)
    let colorSpace = CGColorSpaceCreateDeviceRGB()
    guard let context = CGContext(
        data: nil,
        width: width,
        height: height,
        bitsPerComponent: 8,
        bytesPerRow: 0,
        space: colorSpace,
        bitmapInfo: CGImageAlphaInfo.premultipliedLast.rawValue
    ) else {
        fputs("failed to create context\n", stderr)
        exit(1)
    }
    context.setFillColor(NSColor.white.cgColor)
    context.fill(CGRect(x: 0, y: 0, width: CGFloat(width), height: CGFloat(height)))
    context.saveGState()
    context.translateBy(x: 0, y: CGFloat(height))
    context.scaleBy(x: scale, y: -scale)
    page.draw(with: .mediaBox, to: context)
    context.restoreGState()
    guard let image = context.makeImage() else {
        fputs("failed to render pdf\n", stderr)
        exit(1)
    }
    try recognize(image)
} else {
    guard let image = NSImage(contentsOf: url) else {
        fputs("failed to open image\n", stderr)
        exit(1)
    }
    var rect = CGRect(origin: .zero, size: image.size)
    guard let cgImage = image.cgImage(forProposedRect: &rect, context: nil, hints: nil) else {
        fputs("failed to build cgimage\n", stderr)
        exit(1)
    }
    try recognize(cgImage)
}
