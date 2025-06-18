package tdbg

import (
	"fmt"

	"github.com/urfave/cli/v2"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common/payload"
)

func newPayloadCommands(
	clientFactory ClientFactory,
) []*cli.Command {
	return []*cli.Command{
		{
			Name:  "new",
			Usage: "Create a new payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
			},
			Action: func(c *cli.Context) error {
				return NewPayloadStoreCommand(c, clientFactory)
			},
		},
		{
			Name:  "describe",
			Usage: "Describe a payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
			},
			Action: func(c *cli.Context) error {
				return DescribePayloadStoreCommand(c, clientFactory)
			},
		},
		{
			Name:  "close",
			Usage: "Close a payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
			},
			Action: func(c *cli.Context) error {
				return ClosePayloadStoreCommand(c, clientFactory)
			},
		},
		{
			Name:  "add",
			Usage: "Add payload (string) to a payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
				&cli.StringFlag{
					Name:  FlagKey,
					Usage: "Key for the payload",
				},
				&cli.StringFlag{
					Name:  "payload",
					Usage: "Payload value (string)",
				},
				&cli.Int64Flag{
					Name:  FlagTTL,
					Usage: "Time to live in seconds for the payload",
				},
			},
			Action: func(c *cli.Context) error {
				return AddPayloadCommand(c, clientFactory)
			},
		},
		{
			Name:  "get",
			Usage: "Get payload from a payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
				&cli.StringFlag{
					Name:  FlagKey,
					Usage: "Key for the payload",
				},
			},
			Action: func(c *cli.Context) error {
				return GetPayloadCommand(c, clientFactory)
			},
		},
		{
			Name:  "remove",
			Usage: "Remove payload from a payload store",
			Flags: []cli.Flag{
				&cli.StringFlag{
					Name:  FlagStoreID,
					Usage: "Store ID",
				},
				&cli.StringFlag{
					Name:  FlagKey,
					Usage: "Key for the payload",
				},
			},
			Action: func(c *cli.Context) error {
				return RemovePayloadCommand(c, clientFactory)
			},
		},
	}
}

func NewPayloadStoreCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}
	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.NewPayloadStore(ctx, &adminservice.NewPayloadStoreRequest{
		Namespace: nsName,
		StoreId:   storeID,
	})
	if err != nil {
		return fmt.Errorf("unable to new payload store: %v", err)
	}
	fmt.Println("Payload store created successfully. RunID: ", resp.RunId)
	return nil
}

func DescribePayloadStoreCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}
	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.DescribePayloadStore(ctx, &adminservice.DescribePayloadStoreRequest{
		Namespace: nsName,
		StoreId:   storeID,
	})
	if err != nil {
		return fmt.Errorf("unable to describe payload store: %v", err)
	}
	prettyPrintJSONObject(c, resp)
	return nil
}

func ClosePayloadStoreCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}
	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	_, err = client.ClosePayloadStore(ctx, &adminservice.ClosePayloadStoreRequest{
		Namespace: nsName,
		StoreId:   storeID,
	})
	if err != nil {
		return fmt.Errorf("unable to close payload store: %v", err)
	}
	fmt.Println("Payload store closed successfully.")
	return nil
}

func AddPayloadCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}

	payloadKey := c.String(FlagKey)
	if payloadKey == "" {
		return cli.Exit("Payload key is required", 1)
	}
	payloadString := c.String("payload")
	if payloadString == "" {
		return cli.Exit("Payload value is required", 1)
	}
	payloadValue := payload.EncodeString(payloadString)

	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.AddPayload(ctx, &adminservice.AddPayloadRequest{
		Namespace:  nsName,
		StoreId:    storeID,
		PayloadKey: payloadKey,
		Payload:    payloadValue,
		TtlSeconds: c.Int64(FlagTTL),
	})
	if err != nil {
		return fmt.Errorf("unable to add payload: %v", err)
	}
	fmt.Println(resp)
	return nil
}

func GetPayloadCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}

	payloadKey := c.String(FlagKey)
	if payloadKey == "" {
		return cli.Exit("Payload key is required", 1)
	}

	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.GetPayload(ctx, &adminservice.GetPayloadRequest{
		Namespace:  nsName,
		StoreId:    storeID,
		PayloadKey: payloadKey,
	})
	if err != nil {
		return fmt.Errorf("unable to get payload: %v", err)
	}

	var payloadString string
	payload.Decode(resp.Payload, &payloadString)
	fmt.Println(payloadString)
	return nil
}

func RemovePayloadCommand(c *cli.Context, clientFactory ClientFactory) error {
	nsName, err := getRequiredOption(c, FlagNamespace)
	if err != nil {
		return err
	}

	storeID := c.String(FlagStoreID)
	if storeID == "" {
		return cli.Exit("Store ID is required", 1)
	}

	payloadKey := c.String(FlagKey)
	if payloadKey == "" {
		return cli.Exit("Payload key is required", 1)
	}

	client := clientFactory.AdminClient(c)

	ctx, cancel := newContext(c)
	defer cancel()

	resp, err := client.RemovePayload(ctx, &adminservice.RemovePayloadRequest{
		Namespace:  nsName,
		StoreId:    storeID,
		PayloadKey: payloadKey,
	})
	if err != nil {
		return fmt.Errorf("unable to remove payload: %v", err)
	}
	fmt.Println(resp)
	return nil
}
