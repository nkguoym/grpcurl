package lib

import (
	"context"
	"fmt"
	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/grpcreflect"
	"github.com/nkguoym/grpcurl"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	reflectpb "google.golang.org/grpc/reflection/grpc_reflection_v1alpha"
	"google.golang.org/grpc/status"
	"io"
	"log"
	"os"
	"strings"
	"time"
)

var maxTime time.Duration = 30
var connectTimeout time.Duration = 30 * time.Second
var exit = os.Exit

const statusCodeOffset = 64

type compositeSource struct {
	reflection grpcurl.DescriptorSource
	file       grpcurl.DescriptorSource
}

func (cs compositeSource) ListServices() ([]string, error) {
	return cs.reflection.ListServices()
}

func (cs compositeSource) FindSymbol(fullyQualifiedName string) (desc.Descriptor, error) {
	d, err := cs.reflection.FindSymbol(fullyQualifiedName)
	if err == nil {
		return d, nil
	}
	return cs.file.FindSymbol(fullyQualifiedName)
}

func (cs compositeSource) AllExtensionsForType(typeName string) ([]*desc.FieldDescriptor, error) {
	exts, err := cs.reflection.AllExtensionsForType(typeName)
	if err != nil {
		// On error fall back to file source
		return cs.file.AllExtensionsForType(typeName)
	}
	// Track the tag numbers from the reflection source
	tags := make(map[int32]bool)
	for _, ext := range exts {
		tags[ext.GetNumber()] = true
	}
	fileExts, err := cs.file.AllExtensionsForType(typeName)
	if err != nil {
		return exts, nil
	}
	for _, ext := range fileExts {
		// Prioritize extensions found via reflection
		if !tags[ext.GetNumber()] {
			exts = append(exts, ext)
		}
	}
	return exts, nil
}

func GRPCurl(cmd, target, symbol string, data *string) {
	ctx := context.Background()
	if maxTime > 0 {
		timeout := maxTime * time.Second
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	dial := func() *grpc.ClientConn {
		dialTime := connectTimeout

		ctx, cancel := context.WithTimeout(ctx, dialTime)
		defer cancel()
		var opts []grpc.DialOption

		var creds credentials.TransportCredentials

		grpcurlUA := "grpcurl/1.0"

		opts = append(opts, grpc.WithUserAgent(grpcurlUA))

		network := "tcp"
		//if isUnixSocket != nil && isUnixSocket() {
		//	network = "unix"
		//}
		cc, err := grpcurl.BlockingDial(ctx, network, target, creds, opts...)
		if err != nil {
			log.Fatalf("Failed to dial target host %q, %v", target, err)
		}
		return cc
	}
	printFormattedStatus := func(w io.Writer, stat *status.Status, formatter grpcurl.Formatter) {
		formattedStatus, err := formatter(stat.Proto())
		if err != nil {
			fmt.Fprintf(w, "ERROR: %v", err.Error())
		}
		fmt.Fprint(w, formattedStatus)
	}

	var cc *grpc.ClientConn
	var descSource grpcurl.DescriptorSource
	var refClient *grpcreflect.Client
	var fileSource grpcurl.DescriptorSource

	md := grpcurl.MetadataFromHeaders([]string{""})
	refCtx := metadata.NewOutgoingContext(ctx, md)
	cc = dial()
	refClient = grpcreflect.NewClientV1Alpha(refCtx, reflectpb.NewServerReflectionClient(cc))
	reflSource := grpcurl.DescriptorSourceFromServer(ctx, refClient)
	if fileSource != nil {
		descSource = compositeSource{reflSource, fileSource}
	} else {
		descSource = reflSource
	}

	// arrange for the RPCs to be cleanly shutdown
	reset := func() {
		if refClient != nil {
			refClient.Reset()
			refClient = nil
		}
		if cc != nil {
			cc.Close()
			cc = nil
		}
	}
	defer reset()
	exit = func(code int) {
		// since defers aren't run by os.Exit...
		reset()
		os.Exit(code)
	}

	if cmd == "list" {
		//if symbol == "" {
		//	svcs, err := grpcurl.ListServices(descSource)
		//	if err != nil {
		//		fail(err, "Failed to list services")
		//	}
		//	if len(svcs) == 0 {
		//		fmt.Println("(No services)")
		//	} else {
		//		for _, svc := range svcs {
		//			fmt.Printf("%s\n", svc)
		//		}
		//	}
		//	if err := writeProtoset(descSource, svcs...); err != nil {
		//		fail(err, "Failed to write protoset to %s", *protosetOut)
		//	}
		//} else {
		//	methods, err := grpcurl.ListMethods(descSource, symbol)
		//	if err != nil {
		//		fail(err, "Failed to list methods for service %q", symbol)
		//	}
		//	if len(methods) == 0 {
		//		fmt.Println("(No methods)") // probably unlikely
		//	} else {
		//		for _, m := range methods {
		//			fmt.Printf("%s\n", m)
		//		}
		//	}
		//	if err := writeProtoset(descSource, symbol); err != nil {
		//		fail(err, "Failed to write protoset to %s", *protosetOut)
		//	}
		//}

	} else if cmd == "describe" {
		//var symbols []string
		//if symbol != "" {
		//	symbols = []string{symbol}
		//} else {
		//	// if no symbol given, describe all exposed services
		//	svcs, err := descSource.ListServices()
		//	if err != nil {
		//		fail(err, "Failed to list services")
		//	}
		//	if len(svcs) == 0 {
		//		fmt.Println("Server returned an empty list of exposed services")
		//	}
		//	symbols = svcs
		//}
		//for _, s := range symbols {
		//	if s[0] == '.' {
		//		s = s[1:]
		//	}
		//
		//	dsc, err := descSource.FindSymbol(s)
		//	if err != nil {
		//		fail(err, "Failed to resolve symbol %q", s)
		//	}
		//
		//	fqn := dsc.GetFullyQualifiedName()
		//	var elementType string
		//	switch d := dsc.(type) {
		//	case *desc.MessageDescriptor:
		//		elementType = "a message"
		//		parent, ok := d.GetParent().(*desc.MessageDescriptor)
		//		if ok {
		//			if d.IsMapEntry() {
		//				for _, f := range parent.GetFields() {
		//					if f.IsMap() && f.GetMessageType() == d {
		//						// found it: describe the map field instead
		//						elementType = "the entry type for a map field"
		//						dsc = f
		//						break
		//					}
		//				}
		//			} else {
		//				// see if it's a group
		//				for _, f := range parent.GetFields() {
		//					if f.GetType() == descriptorpb.FieldDescriptorProto_TYPE_GROUP && f.GetMessageType() == d {
		//						// found it: describe the map field instead
		//						elementType = "the type of a group field"
		//						dsc = f
		//						break
		//					}
		//				}
		//			}
		//		}
		//	case *desc.FieldDescriptor:
		//		elementType = "a field"
		//		if d.GetType() == descriptorpb.FieldDescriptorProto_TYPE_GROUP {
		//			elementType = "a group field"
		//		} else if d.IsExtension() {
		//			elementType = "an extension"
		//		}
		//	case *desc.OneOfDescriptor:
		//		elementType = "a one-of"
		//	case *desc.EnumDescriptor:
		//		elementType = "an enum"
		//	case *desc.EnumValueDescriptor:
		//		elementType = "an enum value"
		//	case *desc.ServiceDescriptor:
		//		elementType = "a service"
		//	case *desc.MethodDescriptor:
		//		elementType = "a method"
		//	default:
		//		err = fmt.Errorf("descriptor has unrecognized type %T", dsc)
		//		fail(err, "Failed to describe symbol %q", s)
		//	}
		//
		//	txt, err := grpcurl.GetDescriptorText(dsc, descSource)
		//	if err != nil {
		//		fail(err, "Failed to describe symbol %q", s)
		//	}
		//	fmt.Printf("%s is %s:\n", fqn, elementType)
		//	fmt.Println(txt)
		//
		//	if dsc, ok := dsc.(*desc.MessageDescriptor); ok && *msgTemplate {
		//		// for messages, also show a template in JSON, to make it easier to
		//		// create a request to invoke an RPC
		//		tmpl := grpcurl.MakeTemplate(dsc)
		//		options := grpcurl.FormatOptions{EmitJSONDefaultFields: true}
		//		_, formatter, err := grpcurl.RequestParserAndFormatter(grpcurl.Format(*format), descSource, nil, options)
		//		if err != nil {
		//			fail(err, "Failed to construct formatter for %q", *format)
		//		}
		//		str, err := formatter(tmpl)
		//		if err != nil {
		//			fail(err, "Failed to print template for message %s", s)
		//		}
		//		fmt.Println("\nMessage template:")
		//		fmt.Println(str)
		//	}
		//}
		//if err := writeProtoset(descSource, symbols...); err != nil {
		//	fail(err, "Failed to write protoset to %s", *protosetOut)
		//}

	} else {
		// Invoke an RPC
		if cc == nil {
			cc = dial()
		}
		var in io.Reader
		if *data == "@" {
			in = os.Stdin
		} else {
			in = strings.NewReader(*data)
		}

		includeSeparators := false
		options := grpcurl.FormatOptions{
			EmitJSONDefaultFields: true,
			IncludeTextSeparator:  includeSeparators,
			AllowUnknownFields:    true,
		}
		rf, formatter, err := grpcurl.RequestParserAndFormatter("json", descSource, in, options)
		if err != nil {
			log.Println(err, "Failed to construct request parser and formatter for json")
		}
		h := &grpcurl.DefaultEventHandler{
			Out:            os.Stdout,
			Formatter:      formatter,
			VerbosityLevel: 0,
		}
		formatError := true
		err = grpcurl.InvokeRPC(ctx, descSource, cc, symbol, []string{}, h, rf.Next)
		if err != nil {
			if errStatus, ok := status.FromError(err); ok && formatError {
				h.Status = errStatus
			} else {
				log.Println(err, "Error invoking method %q", symbol)
			}
		}
		reqSuffix := ""
		respSuffix := ""
		reqCount := rf.NumRequests()
		if reqCount != 1 {
			reqSuffix = "s"
		}
		if h.NumResponses != 1 {
			respSuffix = "s"
		}

		fmt.Printf("Sent %d request%s and received %d response%s\n", reqCount, reqSuffix, h.NumResponses, respSuffix)

		if h.Status.Code() != codes.OK {
			if formatError {
				printFormattedStatus(os.Stderr, h.Status, formatter)
			} else {
				grpcurl.PrintStatus(os.Stderr, h.Status, formatter)
			}
			exit(statusCodeOffset + int(h.Status.Code()))
		}
	}
}