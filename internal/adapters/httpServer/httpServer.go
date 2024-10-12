package httpServer

import (
	"context"
	"fmt"
	"io"
	"net/http"

	"github.com/rs/zerolog/log"

	"github.com/labstack/echo/v4"
)

type HttpServer struct {
	port int
}

func New(port int) *HttpServer {
	return &HttpServer{
		port: port,
	}
}

func (hs *HttpServer) Listen(ctx context.Context, responseCh <-chan error) (<-chan []byte, <-chan error) {
	errCh := make(chan error)
	messageCh := make(chan []byte)

	go func() {
		e := echo.New()
		e.POST("/", func(c echo.Context) error {
			b, err := io.ReadAll(c.Request().Body)
			if err != nil {
				err := fmt.Errorf("read body error: %w", err)
				errCh <- err
				return c.JSON(http.StatusInternalServerError, map[string]string{
					"message": err.Error(),
				})
			}

			messageCh <- b
			if err := <-responseCh; err != nil {
				return c.JSON(http.StatusInternalServerError, map[string]string{
					"message": err.Error(),
				})
			}

			return c.JSON(http.StatusOK, map[string]string{
				"message": "ok",
			})
		})

		if err := e.Start(fmt.Sprintf(":%d", hs.port)); err != nil {
			log.Fatal().Err(err).Msg("start router error")
		}
	}()

	return messageCh, errCh
}
