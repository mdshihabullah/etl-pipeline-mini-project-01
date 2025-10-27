"""Notification module for Mastodon data pipeline."""

from .discord_notifier import DiscordNotifier

__all__ = ['DiscordNotifier']

