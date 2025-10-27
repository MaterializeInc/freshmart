import { useEffect, useRef, useState } from 'react';
import { Stack, Text } from '@mantine/core';

const PRICE_FLASH_STYLE_ID = 'price-display-flash-keyframes';

const ensureFlashKeyframes = () => {
  if (typeof document === 'undefined') {
    return;
  }

  if (!document.getElementById(PRICE_FLASH_STYLE_ID)) {
    const style = document.createElement('style');
    style.id = PRICE_FLASH_STYLE_ID;
    style.innerHTML = `
      @keyframes priceFlash {
        0% { background-color: transparent; }
        25% { background-color: rgba(255, 251, 204, 0.1); }
        100% { background-color: transparent; }
      }
    `;
    document.head.appendChild(style);
  }
};

const formatReactionTime = (reactionTime) =>
  reactionTime !== null && reactionTime !== undefined
    ? `As of ${(reactionTime / 1000).toFixed(1)} seconds ago`
    : null;

const PriceDisplay = ({ price, prevPrice, reactionTime, weight = 700, size = 'xl' }) => {
  const priceRef = useRef(null);
  const lastReactionTimeRef = useRef(reactionTime);
  const lastUpdateTimeRef = useRef(Date.now());
  const [dots, setDots] = useState('');

  useEffect(() => {
    ensureFlashKeyframes();
  }, []);

  useEffect(() => {
    if (price !== prevPrice && priceRef.current) {
      priceRef.current.style.animation = 'none';
      // Trigger reflow so animation can restart
      void priceRef.current.offsetHeight; // eslint-disable-line no-unused-expressions
      priceRef.current.style.animation = 'priceFlash 1s ease';
    }
  }, [price, prevPrice]);

  useEffect(() => {
    if (reactionTime !== null && reactionTime !== undefined) {
      lastReactionTimeRef.current = reactionTime;
      lastUpdateTimeRef.current = Date.now();
    }
  }, [reactionTime]);

  useEffect(() => {
    const interval = setInterval(() => {
      setDots((prev) => {
        if (prev === '...') return '';
        return prev + '.';
      });
    }, 500);

    return () => clearInterval(interval);
  }, []);

  const getExtrapolatedReactionTime = () => {
    if (reactionTime !== null && reactionTime !== undefined) {
      return reactionTime;
    }

    if (
      lastReactionTimeRef.current === null ||
      lastReactionTimeRef.current === undefined
    ) {
      return null;
    }

    const timeSinceLastUpdate = Date.now() - lastUpdateTimeRef.current;
    return lastReactionTimeRef.current + timeSinceLastUpdate;
  };

  const displayReactionTime = getExtrapolatedReactionTime();
  const reactionLabel = formatReactionTime(displayReactionTime);

  return (
    <Stack spacing={4} align="center">
      <Text
        ref={priceRef}
        size={size}
        weight={weight}
        color="blue"
        style={{ animation: 'none' }}
      >
        ${price?.toFixed(2) || 'N/A'}
      </Text>
      <Text
        size="xs"
        color="dimmed"
        style={{ minHeight: '20px', opacity: displayReactionTime === null ? 0.7 : 1 }}
      >
        {reactionLabel ?? `Waiting for first response${dots}`}
      </Text>
    </Stack>
  );
};

export default PriceDisplay;
